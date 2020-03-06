package org.lsvh.hu.bdsd.models;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.log4j.Logger;

import java.io.IOException;

public abstract class JobContract {
    private static final Logger log = Logger.getLogger(JobContract.class);
    protected Job job = null;

    protected JobContract(String in, String out) {
        JobConf jobConf = new JobConf();
        FileInputFormat.setInputPaths(jobConf, new Path(in));
        FileOutputFormat.setOutputPath(jobConf, new Path(out));
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
            } else if (job == null) {
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