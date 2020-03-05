package org.lsvh.hu.bdsd.pagerank.jobs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.lsvh.hu.bdsd.pagerank.models.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class CalculateRankJob extends JobContract {
    public CalculateRankJob(String in, String out) {
        super(in, out);
        job.setJarByClass(CalculateRankJob.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        List<Node> nodes;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            nodes = new ArrayList<>();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> tokens = new ArrayList<>();
            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                tokens.add(st.nextToken());
            }
            nodes.add(Node.fromTokens(tokens, true));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node node : nodes) {
                String rank = node.calculateRank().toString();
                context.write(new Text(node.getId()), new Text(Node.ADJACENT_NODE_DEL+node.getJoinedAdjacentNodes()));
                for (String adjacentNode : node.getAdjacentNodes()) {
                    context.write(new Text(adjacentNode), new Text(rank));
                }
            }
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
            double rank = 0.0;
            String adjacentNodes = "";
            for (Text value : values) {
                if (value.toString().contains(Node.ADJACENT_NODE_DEL)) {
                    adjacentNodes = value.toString().substring(1);
                } else {
                    rank += Double.parseDouble(value.toString());
                }
            }
            nodes.add(new Node(key.toString(), rank, Node.fromTokensToAdjacentNodes(adjacentNodes)));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Node n : nodes) {
                context.write(new Text(n.getId()), new Text(n.toString()));
            }
        }
    }
}
