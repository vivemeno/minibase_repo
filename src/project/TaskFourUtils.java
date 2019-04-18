package project;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import bufmgr.BufMgr;
import global.AttrOperator;
import global.AttrType;
import global.NodeTable;
import global.RID;
import heap.Heapfile;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.NestedLoopsJoins;
import iterator.RelSpec;

public class TaskFourUtils {
	public static final int NUMBUF = 10000;
	public static final int TAG_LENGTH = 5;
	private boolean OK = true;
	private boolean FAIL = false;
	public Vector<NodeTable> nodes;
	private String input_file_base = "/home/renil/github/dbmsi/input/";
	private Map<String, String> tagMapping = new HashMap<>(); 
	int wt2NoOfFlds = 0;
	int wt1NoOfFlds = 0;
	
	int queryPlanNumber;
	
	public TaskFourUtils() {}
	
	public TaskFourUtils(int wt1NoOfFlds, int wt2NoOfFlds) {
		this.wt2NoOfFlds = wt2NoOfFlds;
		this.wt1NoOfFlds = wt1NoOfFlds;
	}
	
	public void nestedLoop(Iterator am) {

		short[] Ssizes = new short[wt1NoOfFlds/2];
		for(int i =0; i< Ssizes.length; i++) Ssizes[i] = 5;
		short[] Rsizes = new short[wt2NoOfFlds/2];
		for(int i =0; i< Rsizes.length; i++) Rsizes[i] = 5;
		
		Iterator prevIterator = null;
		NestedLoopsJoins currIterator = null;
		
		AttrType[] baseTableAttrTypes = new AttrType[wt1NoOfFlds];
		for (int i = 0; i <  wt1NoOfFlds; i++) {
			if (i % 2 == 0) {
				baseTableAttrTypes[i] = new AttrType(AttrType.attrString);
			} else {
				baseTableAttrTypes[i] = new AttrType(AttrType.attrInterval);
			}
		}
		
		AttrType[] Rtypes = new AttrType[wt2NoOfFlds];
		for (int i = 0; i <  wt2NoOfFlds; i++) {
			if (i % 2 == 0) {
				Rtypes[i] = new AttrType(AttrType.attrString);
			} else {
				Rtypes[i] = new AttrType(AttrType.attrInterval);
			}
		}
		
		AttrType[] JJtype =  new AttrType[wt1NoOfFlds + wt2NoOfFlds];
		System.arraycopy(baseTableAttrTypes, 0, JJtype, 0, wt1NoOfFlds);
		System.arraycopy(Rtypes, 0, JJtype, wt1NoOfFlds, wt2NoOfFlds);
		
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = 5;

		CondExpr[] innerRelFilterConditions = null;

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		Tuple finalTuple = null;
		
		int k = wt1NoOfFlds + wt2NoOfFlds;
		FldSpec[] proj1 = new FldSpec[k];
		for (int i = 1, j=1; i <=  wt1NoOfFlds + wt2NoOfFlds; i++) {
			if (i <= wt1NoOfFlds) {
				proj1[i-1] = new FldSpec(new RelSpec(RelSpec.outer), i);
			} else {
				proj1[i-1] = new FldSpec(new RelSpec(RelSpec.innerRel), j);
				j++;
			}
		}
		
		NestedLoopsJoins nlj = null;
		try {
			currIterator = new NestedLoopsJoins(baseTableAttrTypes, wt1NoOfFlds, Ssizes, Rtypes, wt2NoOfFlds, Rsizes, 10, am, "witness.in", null, null,
					proj1, k);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
		
		
		try {
			int count = 1;
			while ((finalTuple = currIterator.get_next(1)) != null) {
				System.out.println("Result in CP" + count++ + ":");
				finalTuple.print(JJtype);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		
	}
	
	
}
