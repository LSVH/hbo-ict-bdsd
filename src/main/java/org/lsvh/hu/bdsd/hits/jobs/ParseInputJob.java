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
import java.util.regex.Pattern;

public class ParseInputJob extends JobContract {
    public ParseInputJob(String in, String out) {
        super(in, out);
        job.setJarByClass(ParseInputJob.class);
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
            if (!isComment(value)) {
                nodes.add(Node.fromRawLine(value.toString()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                context.write(new Text(n.getId()), new Text(n.toJson()));
            }
        }

        private static boolean isComment(Text value) {
            return Pattern.compile("^\\s*#").matcher(value.toString()).find();
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
            Node node = null;
            for (Text value : values) {
                if (node == null) {
                    node = Node.fromJsonString(key.toString(), value.toString());
                } else {
                    node.mergeFromJsonString(value.toString());
                }
            }
            nodes.add(node);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                context.write(new Text(n.getId()), new Text(n.toJson()));
            }
        }
    }
}
