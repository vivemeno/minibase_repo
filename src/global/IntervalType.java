package global;
import java.io.Serializable;

public class IntervalType implements Serializable {
    public int s;
    public int e ;
    
    public IntervalType(int start, int end) {
    	this.s = start;
    	this.e = end;
    }
    
    public void assign(int a, int b) {
        this.s = a;
        this.e = b;
    } 
    
    
}
