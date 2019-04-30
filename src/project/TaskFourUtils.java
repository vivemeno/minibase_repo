package project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

import bufmgr.BufMgr;
import global.AttrOperator;
import global.AttrType;
import global.Convert;
import global.GlobalConst;
import global.NodeTable;
import global.RID;
import global.TupleOrder;
import heap.Heapfile;
import heap.Tuple;
import index.IndexException;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.IoBuf;
import iterator.Iterator;
import iterator.JoinsException;
import iterator.NestedLoopsJoins;
import iterator.RelSpec;
import iterator.Sort;
import iterator.SortException;

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
	
	public void init(int wt1NoOfFlds, int wt2NoOfFlds) {
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
		
		AttrType[] JJtype =  new AttrType[wt1NoOfFlds + wt2NoOfFlds + 1];
		JJtype[0] = new AttrType(AttrType.attrString);
		System.arraycopy(baseTableAttrTypes, 0, JJtype, 1, wt1NoOfFlds);
		System.arraycopy(Rtypes, 0, JJtype, wt1NoOfFlds+1, wt2NoOfFlds);
		
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
//			Runtime.getRuntime().exit(1);
		}
		
		//setting attributes for updating tuple header
		short[] tsizes = new short[k/2 + 1];
		for(int i =0; i< tsizes.length; i++) tsizes[i] = 5;
		AttrType[] ttypes = new AttrType[k+1];
		for (int i = 0; i <  k+1; i++) {
			if(i ==0 ) {
				ttypes[i] = new AttrType(AttrType.attrString);
			}else if (i % 2 != 0) {
				ttypes[i] = new AttrType(AttrType.attrString);
			} else {
				ttypes[i] = new AttrType(AttrType.attrInterval);
			}
		}
		currIterator.updateJTUple((short)(k+1), tsizes, ttypes);
		
		try {
			int count = 1;
			while ((finalTuple = currIterator.get_next(1)) != null) {
				System.out.println("Result in CP" + count++ + ":");
				finalTuple.printTreeFormat(JJtype, wt1NoOfFlds);
//				finalTuple.print(JJtype);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			e.printStackTrace();
//			Runtime.getRuntime().exit(1);
		}
		try {
			if(currIterator != null)currIterator.close();
		} catch (JoinsException | IndexException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		currIterator = null;
	}
	
	public void nestedLoopNJOrTJ(Iterator am, int leftNodeNo, int rightNodeNo, String op) {

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
		
		AttrType[] JJtype =  new AttrType[wt1NoOfFlds + wt2NoOfFlds + 1];
		JJtype[0] = new AttrType(AttrType.attrString);
		System.arraycopy(baseTableAttrTypes, 0, JJtype, 1, wt1NoOfFlds);
		System.arraycopy(Rtypes, 0, JJtype, wt1NoOfFlds+1, wt2NoOfFlds);
		
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = 5;

		CondExpr[] innerRelFilterConditions = null;

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		Tuple finalTuple = null;
		
		CondExpr[] expr = new CondExpr[2];
		expr[0] = new CondExpr();
		if(op.equalsIgnoreCase("TJ")) {
			expr[0].operand1.integer = ((leftNodeNo)*2) -1;
			expr[0].operand2.integer = ((rightNodeNo) * 2) -1;
		}else {
			expr[0].operand1.integer = ((leftNodeNo)*2);
			expr[0].operand2.integer = ((rightNodeNo) * 2);
		}
		expr[0].next = null;
	    expr[1] = null;

		
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
			currIterator = new NestedLoopsJoins(baseTableAttrTypes, wt1NoOfFlds, Ssizes, Rtypes, wt2NoOfFlds, Rsizes, 10, am, "witness.in", null, expr,
					proj1, k);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			try {
				if(currIterator != null)currIterator.close();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return;
		}
		
		//setting attributes for updating tuple header
		short[] tsizes = new short[k/2 + 1];
		for(int i =0; i< tsizes.length; i++) tsizes[i] = 5;
		AttrType[] ttypes = new AttrType[k+1];
		for (int i = 0; i <  k+1; i++) {
			if(i ==0 ) {
				ttypes[i] = new AttrType(AttrType.attrString);
			}else if (i % 2 != 0) {
				ttypes[i] = new AttrType(AttrType.attrString);
			} else {
				ttypes[i] = new AttrType(AttrType.attrInterval);
			}
		}
		currIterator.updateJTUple((short)(k+1), tsizes, ttypes);
		
		try {
			int count = 1;
			while ((finalTuple = currIterator.get_next(2)) != null) {
				System.out.println("Result in "+ op + " : " + (count++));
				finalTuple.printTreeFormat(JJtype, wt1NoOfFlds);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			e.printStackTrace();
		}
		try {
			if(currIterator != null)currIterator.close();
		} catch (JoinsException | IndexException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		currIterator = null;
	}
	
	public void sortPhysOP(Iterator am, int nodeNo, int bufsize) throws SortException {
Iterator  p_i2 = null;
		
		short[] Ssizes = new short[wt1NoOfFlds/2];
		for(int i =0; i< Ssizes.length; i++) Ssizes[i] = 5;
		
		AttrType[] baseTableAttrTypes = new AttrType[wt1NoOfFlds];
		for (int i = 0; i <  wt1NoOfFlds; i++) {
			if (i % 2 == 0) {
				baseTableAttrTypes[i] = new AttrType(AttrType.attrString);
			} else {
				baseTableAttrTypes[i] = new AttrType(AttrType.attrInterval);
			}
		}
		
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = 5;

		CondExpr[] innerRelFilterConditions = null;

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		Tuple finalTuple = null;
		
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		
		try {
			p_i2 = new Sort(baseTableAttrTypes, (short)wt1NoOfFlds, Ssizes, am, (nodeNo*2)-1,
					ascending, 5, bufsize);
		}catch(Exception e){
			e.printStackTrace();
			try {
				if(p_i2 != null)p_i2.close();
			} catch (JoinsException | IndexException | IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return;
		}
		
		
		try {
			int count = 1;
			while ((finalTuple = p_i2.get_next()) != null) {
				System.out.println("Result in SRT : " + count++ + ":");
				finalTuple.printTreeFormatSRT(baseTableAttrTypes, wt1NoOfFlds);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			e.printStackTrace();
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
//			Runtime.getRuntime().exit(1);
		}
		try {
			if(p_i2 != null)p_i2.close();
		} catch (JoinsException | IndexException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		p_i2 = null;
	}
	
	
	public Iterator grpSortIterator = null;
	public void grpPhysOP(Iterator am, int nodeNo, int bufsize) throws SortException {
		Iterator  p_i2 = null;
		
		short[] Ssizes = new short[wt1NoOfFlds/2];
		for(int i =0; i< Ssizes.length; i++) Ssizes[i] = 5;
		
		AttrType[] baseTableAttrTypes = new AttrType[wt1NoOfFlds];
		for (int i = 0; i <  wt1NoOfFlds; i++) {
			if (i % 2 == 0) {
				baseTableAttrTypes[i] = new AttrType(AttrType.attrString);
			} else {
				baseTableAttrTypes[i] = new AttrType(AttrType.attrInterval);
			}
		}
		
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = 5;

		CondExpr[] innerRelFilterConditions = null;

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		Tuple finalTuple = null;
		
		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		
		try {
			p_i2 = new Sort(baseTableAttrTypes, (short)wt1NoOfFlds, Ssizes, am, (nodeNo*2)-1,
					ascending, 5, bufsize);
		}catch(Exception e){
			e.printStackTrace();
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				if(p_i2 != null)p_i2.close();
			} catch (JoinsException | IndexException | IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			return;
		}
		
		grpSortIterator = p_i2;
		grpTupleCOunt = 0;
	}
	
	public Tuple grpTuple2 = null;
	int grpTupleCOunt = 0;
	public Tuple get_next_GRP(int nodeNo) {
		
		Tuple Jtuple = new Tuple();
		Tuple finalTuple = new Tuple();
		String prevTag = "", currTag = "";
		int fldno = 2;
		AttrType[] baseTableAttrTypes = null;
		int _n_pages = 1;
		byte[][] _bufs1 = new byte [_n_pages][GlobalConst.MINIBASE_PAGESIZE];
		IoBuf io_buf1 = new IoBuf(); 
		Heapfile temp_file_fd1 = null;
		int count = 0;
		int singleTreeTuplesCount = 0;
		Tuple tempTuple = new Tuple();
		AttrType[] eachTuple_attrs = new AttrType[singleTreeTuplesCount];
		short[] eachTuple_str_sizes = new short[singleTreeTuplesCount/2];
		Tuple insertTup = null;String asdsfd="";
		try {
			
			temp_file_fd1 = new Heapfile(null);
			String pastTupleTag  = null;
			if(grpTuple2 != null) {
				pastTupleTag = grpTuple2.getStrFld((nodeNo*2)-1);
				asdsfd = grpTuple2.getStrFld(7);
			}
			while ((finalTuple = grpSortIterator.get_next()) != null) {
				//System.out.println("Result in GRP" + count++ + ":");
				currTag = finalTuple.getStrFld((nodeNo*2)-1);
				if(grpTuple2 != null) {
					asdsfd = grpTuple2.getStrFld(7);
				}
				if(count == 0) {
					singleTreeTuplesCount = finalTuple.noOfFlds();
					//setting header to that of each tuple
					eachTuple_attrs = new AttrType[singleTreeTuplesCount];
					eachTuple_str_sizes = new short[singleTreeTuplesCount/2];
					for(int i =0, j =0; i< singleTreeTuplesCount; i++) {
						if(i %2 == 0) {
							eachTuple_attrs[i] = new AttrType(AttrType.attrString);
							eachTuple_str_sizes[j] = 5;j++;
						}else {
							eachTuple_attrs[i] = new AttrType(AttrType.attrInterval);
						}
					}
					tempTuple.setHdr(finalTuple.noOfFlds(), eachTuple_attrs, eachTuple_str_sizes);
					io_buf1.init(_bufs1, 1, finalTuple.size(), temp_file_fd1);
					if(grpTuple2 != null && !currTag.equals(pastTupleTag)) {
						count++;
						prevTag = pastTupleTag;
						insertTup = new Tuple();
						insertTup.setHdr((short) singleTreeTuplesCount, eachTuple_attrs, eachTuple_str_sizes);
						insertTup.tupleCopy(grpTuple2);
						io_buf1.Put(insertTup);
						insertTup = new Tuple();
						insertTup.setHdr((short) singleTreeTuplesCount, eachTuple_attrs, eachTuple_str_sizes);
						insertTup.tupleCopy(finalTuple);
						grpTuple2 = insertTup;break;
					}else if(grpTuple2 != null && currTag.equals(pastTupleTag)){
						insertTup = new Tuple();prevTag = pastTupleTag;
						insertTup.setHdr((short) singleTreeTuplesCount, eachTuple_attrs, eachTuple_str_sizes);
						insertTup.tupleCopy(grpTuple2);
						io_buf1.Put(insertTup);grpTuple2 = null;count++;
					}
				}
				
				if(prevTag.equals("") || prevTag.equals(currTag)) {
					count++;
					insertTup = new Tuple();
					insertTup.setHdr((short) singleTreeTuplesCount, eachTuple_attrs, eachTuple_str_sizes);
					insertTup.tupleCopy(finalTuple);
					io_buf1.Put(insertTup);
//					finalTuple.print(eachTuple_attrs);
					
					prevTag =  currTag;
				}else {
					insertTup = new Tuple();
					insertTup.setHdr((short) singleTreeTuplesCount, eachTuple_attrs, eachTuple_str_sizes);
					insertTup.tupleCopy(finalTuple);
					grpTuple2 = insertTup;
					break;
				}
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			e.printStackTrace();
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			try {
				if(grpSortIterator != null)grpSortIterator.close();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			grpSortIterator = null;
			io_buf1 = null;return null;
//			Runtime.getRuntime().exit(1);
		}
		
		if(count == 0 )return null;
		int totalTupWithoutHeader = (count * singleTreeTuplesCount);
		AttrType[] res_attrs = new AttrType[totalTupWithoutHeader + 2];
		short[] res_str_sizes = new short[totalTupWithoutHeader/2 +2];
		try {
		for (int i = 0; i < totalTupWithoutHeader + 2; i++) {
			if(i==0 || i==1) {
				res_attrs[i] = new AttrType(AttrType.attrString);
			}else if(i%2 == 0){
				res_attrs[i] = new AttrType(AttrType.attrString);
			}else{
				res_attrs[i] = new AttrType(AttrType.attrInterval);
			}
		}
		for (int i = 0; i < (totalTupWithoutHeader)/2 +2; i++) {
			if(i == 0) {
				res_str_sizes[i] = 5;
			}else{
				res_str_sizes[i] = 5;
			}
		}
		
		Jtuple.setHdr((short) (totalTupWithoutHeader + 2), res_attrs, res_str_sizes);
		
		Jtuple.setStrFld(1, "groot");
		Jtuple.setStrFld(2, prevTag);
		int k = 2 ;
			while((tempTuple=io_buf1.Get(tempTuple))!= null) {
				for(int i =0; i< tempTuple.noOfFlds(); i++, k++) {
					if(i%2 == 0) {
						Jtuple.setStrFld(k +1 , tempTuple.getStrFld(i + 1));
					}else {
						Jtuple.setIntervalFld(k + 1, tempTuple.getIntervalField(i + 1));							
					}
				}
			}
			
		System.out.println("Result in GRP : " + ++grpTupleCOunt);	
		Jtuple.printTreeFormatGRP(res_attrs, totalTupWithoutHeader + 2, nodeNo);
		io_buf1 = null;
		} catch (Exception e) {
			io_buf1 = null;
			// TODO Auto-generated catch block
			e.printStackTrace();
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		
		
		return Jtuple;
	}
	
	
}
