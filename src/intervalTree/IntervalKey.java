package intervalTree;

import global.IntervalType;

public class IntervalKey extends KeyClass {
	
	 IntervalType key;
	
	public IntervalKey(IntervalType value) {
		key = new IntervalType(value.s, value.e, value.l);

	}
	
	public IntervalType getKey() {
	    return key;
	}
	
	public void setKey(IntervalType value) {
		 key = new IntervalType(value.s, value.e, value.l);
	}

	@Override
	public String toString() {
		
	     return "s: "+key.s+", \n e :" +key.e+", \n l :" +key.l;
	}
	
}
