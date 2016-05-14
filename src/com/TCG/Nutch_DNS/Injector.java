/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.TCG.Nutch_DNS;

import java.io.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

// Commons Logging imports

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import com.TCG.util.*;


public class Injector extends NutchTool implements Tool {
  public static final Logger LOG = LoggerFactory.getLogger(Injector.class);


  /** Normalize  injected urls to host. */
  public static class InjectMapper implements
      Mapper<Text, CrawlDatum, Text, CrawlDatum> {

    public void configure(JobConf job) {
    }

    public void close() {
    }

    public void map(Text key, CrawlDatum value,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {
    	
      URL url = new URL( key.toString() ); // value is line of text
      String tmp = url.getHost();
      Text host = new Text(tmp);

      output.collect(host, value);
    }
  }

  //存在hostDb
  public static class ExitHostReducer implements Reducer<Text, CrawlDatum, Text, CrawlDatum>{

    private long curTime;
    private JobConf jobConf;
    private int interval;

	@Override
	public void configure(JobConf job) {
		// TODO Auto-generated method stub
      this.jobConf = job;
      curTime = job.getLong("injector.current.time", System.currentTimeMillis());
      interval = jobConf.getInt("db.fetch.interval.default", 2592000);
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

    private CrawlDatum datum = new CrawlDatum();

	@Override
	public void reduce(Text key, Iterator<CrawlDatum> values, OutputCollector<Text, CrawlDatum> output, Reporter report)
			throws IOException {
		// TODO Auto-generated method stub
      float sum = 0;
      while(values.hasNext()){
        CrawlDatum val = values.next();
        sum += val.getScore();
      }
      datum.setScore(sum);
      datum.setFetchTime(curTime);
      datum.setFetchInterval(interval);
      datum.setStatus(CrawlDatum.STATUS_INJECTED);//设定Status为INJECTED
	}
	  
  }

  //不存在HostDb
  public static class NotExitHostReducer implements Reducer<Text, CrawlDatum, Text, CrawlDatum>{

    private long curTime;
    private JobConf jobConf;
    private int interval;

    @Override
    public void configure(JobConf job) {
      // TODO Auto-generated method stub
      this.jobConf = job;
      curTime = job.getLong("injector.current.time", System.currentTimeMillis());
      interval = jobConf.getInt("db.fetch.interval.default", 2592000);
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
    }

    private CrawlDatum datum = new CrawlDatum();

    @Override
    public void reduce(Text key, Iterator<CrawlDatum> values, OutputCollector<Text, CrawlDatum> output, Reporter report)
            throws IOException {
      // TODO Auto-generated method stub
      float sum = 0;
      while(values.hasNext()){
        CrawlDatum val = values.next();
        sum += val.getScore();
      }
      datum.setScore(sum);
      datum.setFetchTime(curTime);
      datum.setFetchInterval(interval);
      datum.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);//设定Status为UNFETCHED
    }

  }

  /** Combine multiple new entries for a host. */
  public static class InjectReducer implements
      Reducer<Text, CrawlDatum, Text, CrawlDatum> {
    private int interval;
    private float scoreInjected;
    private boolean overwrite = false;
    private boolean update = false;

    public void configure(JobConf job) {
      interval = job.getInt("db.fetch.interval.default", 2592000);
      scoreInjected = job.getFloat("db.score.injected", 1.0f);
      overwrite = job.getBoolean("db.injector.overwrite", false);
      update = job.getBoolean("db.injector.update", false);
      LOG.info("Injector: overwrite: " + overwrite);
      LOG.info("Injector: update: " + update);
    }

    public void close() {
    }

    private CrawlDatum old = new CrawlDatum();
    private CrawlDatum injected = new CrawlDatum();

    public void reduce(Text key, Iterator<CrawlDatum> values,
        OutputCollector<Text, CrawlDatum> output, Reporter reporter)
        throws IOException {

      boolean oldSet = false;
      boolean injectedSet = false;
      
      while (values.hasNext()) {
        CrawlDatum val = values.next();
        if (val.getStatus() == CrawlDatum.STATUS_INJECTED) {//是新注入的HostDb
          injected.set(val);
          injected.setStatus(CrawlDatum.STATUS_DB_UNFETCHED);
          injectedSet = true;
        } else {//是老HostDb的信息
          old.set(val);
          oldSet = true;
        }

      }

      CrawlDatum res = null;

      // Old default behaviour
      if (injectedSet && !oldSet) {
        res = injected;
      } else {
        res = old;
      }
      if (injectedSet && oldSet) {
        reporter.getCounter("injector", "urls_merged").increment(1);
      }
      /**
       * Whether to overwrite, ignore or update existing records
       * 
       * @see https://issues.apache.org/jira/browse/NUTCH-1405
       */
      // Injected record already exists and update but not overwrite
      if (injectedSet && oldSet && update && !overwrite) {
        res = old;
        old.putAllMetaData(injected);
        old.setScore(injected.getScore() != scoreInjected ? injected.getScore()
            : old.getScore());
        old.setFetchInterval(injected.getFetchInterval() != interval ? injected
            .getFetchInterval() : old.getFetchInterval());
      }

      // Injected record already exists and overwrite
      if (injectedSet && oldSet && overwrite) {
        res = injected;
      }

      output.collect(key, res);
    }
  }

  public Injector() {
  }

  public Injector(Configuration conf) {
    setConf(conf);
  }

  public void inject(Path hostDb, Path crawlDb) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long start = System.currentTimeMillis();
    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: starting at " + sdf.format(start));
      LOG.info("Injector: hostDb: " + hostDb);
      LOG.info("Injector: carwlDb: " + crawlDb);
    }

    Path tempDir = new Path(getConf().get("mapred.temp.dir", ".")
        + "/inject-temp-"
        + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    // map text input file to a <url,CrawlDatum> file
    if (LOG.isInfoEnabled()) {
      LOG.info("Injector: Converting injected host to host db entries.");
    }

    FileSystem fs = FileSystem.get(getConf());
    // determine if the crawldb already exists
    boolean dbExists = fs.exists(hostDb);

    JobConf sortJob = new NutchJob(getConf());
    sortJob.setJobName("inject " + hostDb);
    FileInputFormat.addInputPath(sortJob, crawlDb);
    sortJob.setMapperClass(InjectMapper.class);

    FileOutputFormat.setOutputPath(sortJob, tempDir);
    if (dbExists) {
      sortJob.setOutputFormat(SequenceFileOutputFormat.class);
      //增加HostReducer,计算host的得分
      sortJob.setReducerClass(ExitHostReducer.class);
    } else {
      sortJob.setOutputFormat(MapFileOutputFormat.class);

      //增加HostReducer,计算host的得分
      sortJob.setReducerClass(NotExitHostReducer.class);

      sortJob.setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs",
          false);
    }
    sortJob.setOutputKeyClass(Text.class);
    sortJob.setOutputValueClass(CrawlDatum.class);
    sortJob.setLong("injector.current.time", System.currentTimeMillis());

    RunningJob mapJob = null;
    try {
      mapJob = JobClient.runJob(sortJob);
    } catch (IOException e) {
      fs.delete(tempDir, true);
      throw e;
    }

    if (dbExists) {

      // merge with existing host db
      if (LOG.isInfoEnabled()) {
        LOG.info("Injector: Merging injected hostDb into old hostDb.");
      }
      JobConf mergeJob = HostDb.createJob(getConf(), hostDb);
      FileInputFormat.addInputPath(mergeJob, tempDir);
      //会覆盖HostDb.createJob中指定的Reducer:HostDbReducer
      mergeJob.setReducerClass(InjectReducer.class);
      try {
        RunningJob merge = JobClient.runJob(mergeJob);
      } catch (IOException e) {
        fs.delete(tempDir, true);
        throw e;
      }
      HostDb.install(mergeJob, hostDb);
    } else {
      HostDb.install(sortJob, hostDb);
    }

    // clean up
    fs.delete(tempDir, true);

    long end = System.currentTimeMillis();
    LOG.info("Injector: finished at " + sdf.format(end) + ", elapsed: "
        + TimingUtil.elapsedTime(start, end));
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(NutchConfiguration.create(), new Injector(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: Injector <hostdb> <crawldb>");
      return -1;
    }
    try {
      inject(new Path(args[0]), new Path(args[1]));
      return 0;
    } catch (Exception e) {
      LOG.error("Injector: " + StringUtils.stringifyException(e));
      return -1;
    }
  }

  @Override
  /**
   * Used by the Nutch REST service
   */
  public Map<String, Object> run(Map<String, String> args, String hostId) throws Exception {
    if(args.size()<1){
      throw new IllegalArgumentException("Required arguments <url_dir>");
    }
    Map<String, Object> results = new HashMap<String, Object>();
    String RESULT = "result";
    String hostdb = hostId+"/hostdb";
    String crawldb = args.get("crawldb");

    inject(new Path(hostdb), new Path(crawldb));
    results.put(RESULT, Integer.toString(0));
    return results;

  }

}
