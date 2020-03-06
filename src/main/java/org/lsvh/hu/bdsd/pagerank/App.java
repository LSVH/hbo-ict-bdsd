package org.lsvh.hu.bdsd.pagerank;

import org.lsvh.hu.bdsd.MapReduceApp;
import org.lsvh.hu.bdsd.models.JobThread;
import org.lsvh.hu.bdsd.pagerank.jobs.CalculateRankJob;
import org.lsvh.hu.bdsd.pagerank.jobs.ParseInputJob;

import java.io.File;
import java.util.*;

public class App extends MapReduceApp {
    private static final String PARSED = File.separator + "parsed";
    private static final String RANKED = File.separator + "ranked";

    public static void main(String[] args) {
        setupLogging();
        setupCLI(args);
        appendToOutputPath("pageRank");
        setupIO();
        setupJobs();
    }

    private static void setupJobs() {
        new JobThread(new ParseInputJob(in, out + PARSED), loadCalculateRankJobs()).start();
    }

    private static Thread loadCalculateRankJobs() {
        String previouslyParsed = out + PARSED;
        Thread thread = null;
        List<CalculateRankJob> jobs = new ArrayList<>();
        for (int i = 0; i < its; i++) {
            String output = out + RANKED + File.separator + i;
            jobs.add(new CalculateRankJob(previouslyParsed, output));
            previouslyParsed = output;
        }
        Collections.reverse(jobs);
        for (CalculateRankJob job : jobs) {
            thread = new JobThread(job, thread);
        }
        return thread;
    }
}
