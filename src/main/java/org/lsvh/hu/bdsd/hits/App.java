package org.lsvh.hu.bdsd.hits;

import org.lsvh.hu.bdsd.MapReduceApp;

import java.io.File;

public class App extends MapReduceApp {
    private static final String PARSED = File.separator + "parsed";
    private static final String RANKED = File.separator + "ranked";

    public static void main(String[] args) {
        setupLogging();
        setupCLI(args);
        appendToOutputPath("hits");
        setupIO();
        setupJobs();
    }

    private static void setupJobs() {

    }
}
