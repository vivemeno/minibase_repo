package tests;

import global.IntervalType;

public class NodeTable {
    public String nodename;
    public IntervalType interval;

    public NodeTable () {

    }

    public NodeTable (String _nodeName, IntervalType _interval) {
        nodename = _nodeName;
        interval = _interval;
    }
}
