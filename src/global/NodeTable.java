package global;

import global.IntervalType;

//adding NodeTable schema
public class NodeTable {
	public String nodename;
	public IntervalType interval;
	
	public NodeTable (String _nodeName, IntervalType _interval) {
		nodename = _nodeName;
		interval = _interval;
	}
	
	public NodeTable() {
		
	}
}