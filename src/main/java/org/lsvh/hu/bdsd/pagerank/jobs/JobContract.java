package org.lsvh.hu.bdsd.pagerank.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class JobContract {
    private static final Logger log = Logger.getLogger(JobContract.class);
    protected Job job = null;

    public JobContract(Path in, Path out) {
        JobConf jobConf = new JobConf();
        FileInputFormat.setInputPaths(jobConf, in);
        FileOutputFormat.setOutputPath(jobConf, out);
        try {
            job = Job.getInstance(jobConf);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } catch (Exception e) {
            log.error(e);
        }
    }

    public void run() {
        try {
            if (isRunning()) {
                throw new IOException("Job is already running");
            } else  if (job == null) {
                throw new IOException("Job is not defined");
            } else {
                job.waitForCompletion(true);
            }
        } catch (Exception e) {
            log.error(e);
        }
    }

    public boolean isRunning() {
        try {
            return job != null && job.getStatus().getState() == JobStatus.State.RUNNING;
        } catch (Exception e) {
            return false;
        }
    }

}