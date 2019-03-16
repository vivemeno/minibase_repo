package global;
import java.io.Serializable;

public class IntervalType implements Serializable {
    public int s; //Start Interval
    public int e; //End Interval
    public int l; //Level of current interval node

    public IntervalType() {

    }

    public IntervalType(int start, int end, int level) {
        this.s = start;
        this.e = end;
        this.l = level;
    }

    public void assign(int a, int b) {
        this.s = a;
        this.e = b;
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) return true;
        if (o == null || !(o instanceof IntervalType)) {
            return false;
        }

        IntervalType intrvl = (IntervalType) o;

        return intrvl.s == s &&
                intrvl.e == e;
    }


    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + s;
        result = 31 * result + e;
        return result;
    }


}
