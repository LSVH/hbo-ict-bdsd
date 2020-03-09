package org.lsvh.hu.bdsd.hits;

public class NodeObject {
    private Double auth = 0.0;
    private Double hub = 0.0;
    private String[] out;
    private String[] in;

    public NodeObject(Double auth, Double hub, String[] out, String[] in) {
        this.auth = auth;
        this.hub = hub;
        this.out = out;
        this.in = in;
    }

    public Double getAuth() {
        return auth;
    }

    public Double getHub() {
        return hub;
    }

    public String[] getOutNodes() {
        return out;
    }

    public String[] getInNodes() {
        return in;
    }

    public Node convertToNode(String id) {
        Node n = new Node(id);
        n.setAuth(auth);
        n.setHub(hub);
        n.setOutNodes(out);
        n.setInNodes(in);
        return n;
    }
}
