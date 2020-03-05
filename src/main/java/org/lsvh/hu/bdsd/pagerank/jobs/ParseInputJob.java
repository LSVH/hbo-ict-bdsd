package org.lsvh.hu.bdsd.pagerank.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.lsvh.hu.bdsd.pagerank.models.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class ParseInputJob extends JobContract {
    public ParseInputJob(Path in, Path out) {
        super(in, out);
        job.setJarByClass(ParseInputJob.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private List<Node> nodes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            nodes = new ArrayList<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!isComment(value)) {
                List<String> tokens = new ArrayList<>();
                StringTokenizer st = new StringTokenizer(value.toString());
                while (st.hasMoreTokens()) {
                    tokens.add(st.nextToken());
                }
                nodes.add(Node.fromTokens(tokens));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                context.write(new Text(n.getId()), new Text(n.getJoinedAdjacentNodes()));
            }
        }

        private static boolean isComment(Text value) {
            return Pattern.compile("^\\s*#").matcher(value.toString()).find();
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        List<Node> nodes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            nodes = new ArrayList<>();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> tokens = new ArrayList<>();
            for (Text value : values) {
                StringTokenizer st = new StringTokenizer(value.toString(), Node.ADJACENT_NODE_DEL);
                while (st.hasMoreTokens()) {
                    tokens.add(st.nextToken());
                }
            }
            nodes.add(new Node(key.toString(), tokens));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                n.setRank(1D / nodes.size());
                context.write(new Text(n.getId()), new Text(n.toString()));
            }
        }
    }
}
