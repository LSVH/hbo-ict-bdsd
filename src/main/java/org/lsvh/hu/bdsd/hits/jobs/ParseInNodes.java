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

public class ParseInNodes extends JobContract {
    public ParseInNodes(String in, String out) {
        super(in, out);
        job.setJarByClass(ParseInNodes.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private List<Node> nodes;

        @Override
        protected void setup(Context context) {
            nodes = new ArrayList<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            Node n = Node.fromParsedLine(value.toString());
            nodes.add(n);
            for (String outNode : n.getOutNodes()) {
                Node in = new Node(outNode);
                in.setInNodes(new String[]{n.getId()});
                nodes.add(in);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                context.write(new Text(n.getId()), new Text(n.toJson()));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        List<Node> nodes;

        @Override
        protected void setup(Context context) {
            nodes = new ArrayList<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) {
            Node n = new Node(key.toString());
            for (Text value : values) {
                n.mergeFromJsonString(value.toString());
            }
            nodes.add(n);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                context.write(new Text(n.getId()), new Text(n.toJson()));
            }
        }
    }
}
