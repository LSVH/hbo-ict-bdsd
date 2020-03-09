package org.lsvh.hu.bdsd.hits.jobs;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.lsvh.hu.bdsd.hits.Node;
import org.lsvh.hu.bdsd.models.JobContract;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CalculateAuthJob extends JobContract {
    protected static Map<String, Double> hubs = new HashMap<>();

    public CalculateAuthJob(String in, String out) {
        super(in, out);
        job.setJarByClass(CalculateAuthJob.class);
        job.setMapperClass(MapIt.class);
        job.setReducerClass(ReduceIt.class);
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
            hubs.put(n.getId(), n.getHub());
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
                Double auth = 0.0;
                for (String inNode : node.getInNodes()) {
                    auth += hubs.get(inNode);
                }
                node.setAuth(auth);
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
