package intervalTree;

import global.IntervalType;

public class CompositeKey extends KeyClass {
	
	 public IntervalType key;
	 public String name;
	
	public CompositeKey(IntervalType value) {
		key = new IntervalType(value.s, value.e, value.l);

	}
	
	public CompositeKey(IntervalType value, String name) {
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
