package global;
import java.io.Serializable;

public class IntervalType implements Serializable {
    public int s; //Start Interval
    public int e; //End Interval
    public int l; //Level of current interval node
    
    public IntervalType(int start, int end, int level) {
    	this.s = start;
    	this.e = end;
    	this.l = level;
    }
    
    public void assign(int a, int b) {
        this.s = a;
        this.e = b;
    } 
    
    
}
