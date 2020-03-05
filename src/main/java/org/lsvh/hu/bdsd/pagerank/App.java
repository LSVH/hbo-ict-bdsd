package org.lsvh.hu.bdsd.pagerank;

import org.lsvh.hu.bdsd.MapReduceApp;
import org.lsvh.hu.bdsd.pagerank.jobs.ParseInputJob;

public class App extends MapReduceApp {
    public static void main(String[] args) {
        setupLogging();
        setupCLI(args);
        setupIO();
        setupJobs();
    }

    private static void setupJobs() {
        new ParseInputJob(in, out).run();
    }
}
