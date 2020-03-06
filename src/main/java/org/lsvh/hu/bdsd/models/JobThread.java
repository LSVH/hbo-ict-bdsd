package org.lsvh.hu.bdsd.models;

public class JobThread extends Thread {
    private JobContract job;
    private Thread next;

    public JobThread(JobContract job, Thread next) {
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
