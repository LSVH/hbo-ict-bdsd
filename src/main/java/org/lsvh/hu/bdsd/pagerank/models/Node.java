package org.lsvh.hu.bdsd.pagerank.models;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class Node {
    private String id;
    private Double rank = 0.0;
    private List<String> adjacentNodes;
    public static final String ADJACENT_NODE_DEL = ";";

    public Node(String id, List<String> adjacentNodes) {
        this.id = id;
        this.adjacentNodes = adjacentNodes;
    }

    public Node(String id, Double rank, List<String> adjacentNodes) {
        this.id = id;
        this.rank = rank;
        this.adjacentNodes = adjacentNodes;
    }

    public static Node fromTokens(List<String> tokens) {
        return fromTokens(tokens, false);
    }

    public static Node fromTokens(List<String> tokens, boolean withRank) {
        String id = tokens.get(0);
        tokens.remove(0);
        if (withRank) {
            Double rank = Double.parseDouble(tokens.get(0));
            tokens.remove(0);
            return new Node(id, rank, lastTokensToAdjacentNodes(tokens));
        } else {
            return new Node(id, lastTokensToAdjacentNodes(tokens));
        }
    }

    public static List<String> fromTokensToAdjacentNodes(String tokens) {
        List<String> adjacentNodes = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(tokens, ADJACENT_NODE_DEL);
        while (st.hasMoreTokens()) {
            adjacentNodes.add(st.nextToken());
        }
        return adjacentNodes;
    }

    private static List<String> lastTokensToAdjacentNodes(List<String> tokens) {
        return fromTokensToAdjacentNodes(String.join(ADJACENT_NODE_DEL, tokens));
    }

    public String getId() {
        return id;
    }

    public void setRank(Double rank) {
        this.rank = rank;
    }

    public List<String> getAdjacentNodes() {
        return adjacentNodes;
    }

    public Double calculateRank() {
        return !this.adjacentNodes.isEmpty() ? this.rank / this.adjacentNodes.size() : 0.0;
    }

    public String getJoinedAdjacentNodes() {
        return String.join(ADJACENT_NODE_DEL, this.adjacentNodes);
    }

    @Override
    public String toString() {
        return String.format("%.21f\t%s", this.rank, getJoinedAdjacentNodes());
    }
}
