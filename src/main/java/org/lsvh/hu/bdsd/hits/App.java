package org.lsvh.hu.bdsd.hits;

import org.lsvh.hu.bdsd.MapReduceApp;
import org.lsvh.hu.bdsd.hits.jobs.*;
import org.lsvh.hu.bdsd.models.JobThread;

import java.io.File;

public class App extends MapReduceApp {
    private static final String READ = File.separator + "read";
    private static final String PARSED = File.separator + "parsed";
    private static final String RANKED = File.separator + "ranked";
    private static final String AUTHED = File.separator + "authed";
    private static final String HUBBED = File.separator + "hubbed";
    private static final String NORMAL = File.separator + "normalized";

    public static void main(String[] args) {
        setupLogging();
        setupCLI(args);
        appendToOutputPath("hits");
        setupIO();
        setupJobs();
    }

    private static void setupJobs() {
        new JobThread(
                new ParseInputJob(in, out + READ),
                new JobThread(
                        new ParseInNodes(out + READ, out + PARSED),
                        getIterativeJob(out + PARSED, its)
                )
        ).start();
    }
    
    private static JobThread getIterativeJob(String in, int iterations) {
        return getIterativeJob(in, iterations, 0);
    }

    private static JobThread getIterativeJob(String in, int iterations, int current) {
        String outAuth = out + RANKED + File.separator + current + AUTHED;
        String outHub = out + RANKED + File.separator + current + HUBBED;
        String outNorm = out + RANKED + File.separator + current + NORMAL;
        return current < iterations ? new JobThread(
            new CalculateAuthJob(in, outAuth),
            new JobThread(
                new CalculateHubJob(outAuth, outHub),
                new JobThread(
                    new NormalizeJob(outHub, outNorm),
                    getIterativeJob(outNorm, iterations, current+1)
                )
            )
        ) : null;
    }
}
