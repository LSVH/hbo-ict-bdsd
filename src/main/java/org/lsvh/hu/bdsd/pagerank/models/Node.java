package org.lsvh.hu.bdsd.pagerank.models;

import java.util.ArrayList;
import java.util.List;

public class Node {
    private String id;
    private double rank = 0;
    private List<String> adjacentNodes = new ArrayList<>();
    public static final String ADJACENT_NODE_DEL = ";";

    public Node(String id) {
        this.id = id;
    }

    public Node(String id, List<String> adjacentNodes) {
        this.id = id;
        this.adjacentNodes = adjacentNodes;
    }

    public static Node fromTokens(List<String> tokens) {
        String id = tokens.get(0);
        tokens.remove(0);
        return new Node(id, tokens);
    }

    public String getId() {
        return id;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public String getJoinedAdjacentNodes() {
        return String.join(ADJACENT_NODE_DEL, this.adjacentNodes);
    }

    @Override
    public String toString() {
        return String.format("%f\t%s", this.rank, getJoinedAdjacentNodes());
    }
}
