package org.lsvh.hu.bdsd;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

public abstract class MapReduceApp {
    protected static final Logger log = Logger.getLogger(MapReduceApp.class);

    private static final String OPT_IN = "input";
    private static final String OPT_OUT = "output";
    private static final String OPT_ITS = "iterations";
    private static final String INPUT_PATH = "data" + File.separator + OPT_IN;
    private static final String OUTPUT_PATH = "data" + File.separator + OPT_OUT;
    private static final Integer JOB_ITERATIONS = 10;

    protected static String in;
    protected static String out;
    protected static Integer its;

    protected MapReduceApp() {
    }

    protected static void appendToOutputPath(String suf) {
        out = out + File.separator + removeTrailingSlash(suf);
    }

    protected static void setupLogging() {
        org.apache.log4j.BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.ERROR);
    }

    protected static void setupCLI(String[] args) {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();

        options.addOption("i", OPT_IN, true, "what files should be parsed");
        options.addOption("o", OPT_OUT, true, "where the parsed files should be stored");
        options.addOption("I", OPT_ITS, true, "how many times the job(s) should run");

        try {
            CommandLine line = parser.parse(options, args);
            in = removeTrailingSlash(line.hasOption(OPT_IN) ? line.getOptionValue(OPT_IN) : INPUT_PATH);
            out = removeTrailingSlash(line.hasOption(OPT_OUT) ? line.getOptionValue(OPT_OUT) : OUTPUT_PATH);
            its = line.hasOption(OPT_ITS) ? Integer.parseInt(line.getOptionValue(OPT_ITS)) : JOB_ITERATIONS;
        } catch (Exception e) {
            log.error(e);
        }
    }

    protected static void setupIO() {
        File outputDir = new File(out);
        if (outputDir.isDirectory()) {
            try {
                FileUtils.deleteDirectory(outputDir);
            } catch (IOException e) {
                log.error(e);
            }
        }
    }

    private static String removeTrailingSlash(String path) {
        return path.replaceAll("[/\\\\]$", "");
    }
}
