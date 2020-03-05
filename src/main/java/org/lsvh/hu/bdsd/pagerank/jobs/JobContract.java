package org.lsvh.hu.bdsd.pagerank.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;

import java.io.IOException;

public abstract class JobContract {
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
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            if (isRunning()) {
                throw new IOException("Job is already running");
            } else  if (job == null) {
                throw new IOException("Job is not defined");
            } else {
                System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
                System.out.println("-=-=-=-=-=-=-=-=-=-     RUNNING JOB     -=-=-=-=-=-=-=-=-=-=-");
                System.out.println("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
                job.waitForCompletion(true);
            }
        } catch (Exception e) {
            e.printStackTrace();
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