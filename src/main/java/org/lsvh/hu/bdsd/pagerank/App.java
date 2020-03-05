package org.lsvh.hu.bdsd.pagerank;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.lsvh.hu.bdsd.pagerank.jobs.ParseInputJob;

import java.io.File;
import java.io.IOException;

public class App {
    private static final String INPUT_PATH = "data/input";
    private static final String OUTPUT_PATH = "data/output";
    private static final Integer JOB_ITERATIONS = 10;
    private static final String OPT_IN = "input";
    private static final String OPT_OUT = "output";
    private static final String OPT_ITS = "iterations";

    private static Path in;
    private static Path out;
    private static Integer its;

    public static void main(String[] args) {
        org.apache.log4j.BasicConfigurator.configure();

        CommandLineParser parser = new DefaultParser();
        Options options = new Options();

        options.addOption("i", OPT_IN, true, "what files should be parsed");
        options.addOption("o", OPT_OUT, true, "where the parsed files should be stored");
        options.addOption("I", OPT_ITS, true, "how many times the job(s) should run");

        try {
            CommandLine line = parser.parse(options, args);
            in = new Path(line.hasOption(OPT_IN) ? line.getOptionValue(OPT_IN) : INPUT_PATH);
            out = new Path(line.hasOption(OPT_OUT) ? line.getOptionValue(OPT_OUT) : OUTPUT_PATH);
            its = line.hasOption(OPT_ITS) ? Integer.parseInt(line.getOptionValue(OPT_ITS)) : JOB_ITERATIONS;
            setup();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setup() {
        setupLogging();
        setupIO();
        setupJobs();
    }

    private static void setupLogging() {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    private static void setupIO() {
        File outputDir = new File(out.toString());
        if (outputDir.isDirectory()) {
            try {
                FileUtils.deleteDirectory(outputDir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void setupJobs() {
        ParseInputJob parseInput = new ParseInputJob(in, out);
        parseInput.run();
    }
}
