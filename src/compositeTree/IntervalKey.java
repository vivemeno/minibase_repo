package compositeTree;

import global.IntervalType;

public class IntervalKey extends KeyClass {
	
	 public IntervalType key;
	 public String name;
	
	public IntervalKey(IntervalType value) {
		key = new IntervalType(value.s, value.e, value.l);
	}
	
	public IntervalKey(IntervalType value, String name) {
		key = new IntervalType(value.s, value.e, value.l);
		this.name = name;

	}
	
	public IntervalType getKey() {
	    return key;
	}
	
	public void setKey(IntervalType value) {
		 key = new IntervalType(value.s, value.e, value.l);
	}

	@Override
	public String toString() {
		
	     return "["+"s: "+key.s+", e :" +key.e+", l :" +key.l + "] tagName: "+ name;
	}
	
}
