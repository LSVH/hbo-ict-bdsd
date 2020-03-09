package org.lsvh.hu.bdsd.hits.jobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.lsvh.hu.bdsd.hits.Node;
import org.lsvh.hu.bdsd.models.JobContract;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NormalizeJob extends JobContract {
    protected static double hub = 0.0;
    protected static double auth = 0.0;

    public NormalizeJob(String in, String out) {
        super(in, out);
        job.setJarByClass(NormalizeJob.class);
        job.setMapperClass(MapIt.class);
        job.setReducerClass(ReduceIt.class);
    }

    protected static void addToHub(double hub) {
        NormalizeJob.hub += hub;
    }

    protected static void addToAuth(double auth) {
        NormalizeJob.auth += auth;
    }

    public static class MapIt extends Mapper<LongWritable, Text, Text, Text> {
        private List<Node> nodes;

        @Override
        protected void setup(Context context) {
            nodes = new ArrayList<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            Node n = Node.fromParsedLine(value.toString());
            nodes.add(n);
            addToHub(n.getHub());
            addToAuth(n.getAuth());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                context.write(new Text(n.getId()), new Text(n.toJson()));
            }
        }
    }

    public static class ReduceIt extends Reducer<Text, Text, Text, Text> {
        List<Node> nodes;

        @Override
        protected void setup(Context context) {
            nodes = new ArrayList<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            Node node = null;
            for (Text value : values) {
                node = Node.fromJsonString(key.toString(), value.toString());
                node.setHub(node.getHub()/hub);
                node.setAuth(node.getAuth()/auth);
                nodes.add(node);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                context.write(new Text(n.getId()), new Text(n.toJson()));
            }
        }
    }
}
