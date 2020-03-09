package org.lsvh.hu.bdsd.hits;

import com.google.gson.Gson;
import org.apache.commons.lang3.ArrayUtils;

import java.util.*;

public class Node {
    private String id;
    private Double auth = 1.0;
    private Double hub = 1.0;
    private String[] outNodes;
    private String[] inNodes;
    public static final String NODE_DEL = ";";

    public Node(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public Double getAuth() {
        return auth;
    }

    public Double getHub() {
        return hub;
    }

    public String[] getOutNodes() {
        return outNodes;
    }

    public String[] getInNodes() {
        return inNodes;
    }

    public void setAuth(Double auth) {
        this.auth = auth;
    }

    public void setHub(Double hub) {
        this.hub = hub;
    }

    public void setOutNodes(String[] outNodes) {
        this.outNodes = outNodes;
    }

    public void setInNodes(String[] inNodes) {
        this.inNodes = inNodes;
    }

    public void calculateAuth(List<Node> nodes) {
        this.auth = calculateScore(nodes, 0);
    }

    public void calculateHub(List<Node> nodes) {
        this.hub = calculateScore(nodes, 1);
    }

    public String toJson() {
        NodeObject nodeObject = new NodeObject(this.auth, this.hub, this.outNodes, this.inNodes);
        return new Gson().toJson(nodeObject);
    }

    public void mergeFromJsonString(String json) {
        NodeObject nodeObject = new Gson().fromJson(json, NodeObject.class);
        this.outNodes = mergeArray(this.outNodes, nodeObject.getOutNodes());
        this.inNodes = mergeArray(this.inNodes, nodeObject.getInNodes());
        this.auth = nodeObject.getAuth();
        this.hub = nodeObject.getHub();
    }

    public static Node fromRawLine(String line) {
        List<String> tokens = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(line);
        while (st.hasMoreTokens()) {
            tokens.add(st.nextToken());
        }
        Node n = new Node(tokens.get(0));
        n.setOutNodes(Collections.singletonList(tokens.get(1)).toArray(String[]::new));
        return n;
    }

    public static Node fromParsedLine(String line) {
        List<String> tokens = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(line);
        while (st.hasMoreTokens()) {
            tokens.add(st.nextToken());
        }
        return fromJsonString(tokens.get(0), tokens.get(1));
    }

    public static Node fromJsonString(String id, String json) {
        NodeObject nodeObject = new Gson().fromJson(json, NodeObject.class);
        return nodeObject.convertToNode(id);
    }

    private Double calculateScore(List<Node> nodes, int scoreType) {
        Double r = 0.0;
        for (Node n : nodes) {
            r += scoreType == 0 ? n.getHub() : n.getAuth();
        }
        return r / nodes.size();
    }

    private static String[] mergeArray(String[] ...arrays) {
        Set<String> set = new HashSet<>();
        for (String[] array : arrays) {
            if (array != null && array.length > 0) {
                List<String> list = Arrays.asList(array);
                set.addAll(list);
            }
        }
        return set.toArray(String[]::new);
    }

    @Override
    public String toString() {
        return "Node{" +
                "id='" + id + '\'' +
                ", auth=" + auth +
                ", hub=" + hub +
                ", outNodes=" + Arrays.toString(outNodes) +
                ", inNodes=" + Arrays.toString(inNodes) +
                '}';
    }
}
