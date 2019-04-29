package compositeTree;

import java.io.IOException;
import global.*;

public class IntervalTreeScan {
	
	IntervalFileScan intervalFileScan= null;
	
	static void traceFilename(String filename) {

	}
	
	public IntervalFileScan new_scan(String filename) {
		return intervalFileScan;
	}
	
	/*
	 * 0 - before 
	 * 1 - meets 
	 * 2 - overlaps 
	 * 3 - starts 
	 * 4 - contained-by 
	 * 5 - finishes 
	 * 6- started-by 
	 * 7 - contains 
	 * 8 - finished-by 
	 * 9 - overlapped-by 
	 * 10- met-by 
	 * 11-after 
	 * 12- equal
	 */
	public IntervalFileScan new_scan(String fileName, KeyClass key, int condition /* we care about 4, 7, 12 */) throws IOException,  
		   KeyNotMatchException, 
		   IteratorException, 
		   ConstructPageException, 
		   PinPageException, 
		   UnpinPageException, GetFileEntryException
		   
	 {
		IntervalTreeFile iTreeFile = new IntervalTreeFile(fileName);
		intervalFileScan = iTreeFile.new_scan(key, null, condition);
		return intervalFileScan;
		
	}
	
	
	

}
