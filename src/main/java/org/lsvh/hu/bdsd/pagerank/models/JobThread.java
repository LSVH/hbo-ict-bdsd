package org.lsvh.hu.bdsd.pagerank.models;

import org.lsvh.hu.bdsd.pagerank.jobs.JobContract;

public class JobThread extends Thread {
    JobContract job;
    JobThread next;

    public JobThread(JobContract job, JobThread next) {
        this.job = job;
        this.next = next;
    }

    public void run() {
        job.run();
        if (next != null) {
            while (!job.isRunning()) {
                next.start();
                break;
            }
        }
    }
}
