package tests;
//originally from : joins.C

import iterator.*;
import iterator.Iterator;
import heap.*;
import global.*;
import index.*;
import java.io.*;
import java.util.*;
import java.lang.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;
import catalog.*;

/**
 Here is the implementation for the tests. There are N tests performed.
 We start off by showing that each operator works on its own.
 Then more complicated trees are constructed.
 As a nice feature, we allow the user to specify a selection condition.
 We also allow the user to hardwire trees together.
 */

//Define the Sailor schema
class Sailor {
	public int    sid;
	public String sname;
	public int    rating;
	public double age;

	public Sailor (int _sid, String _sname, int _rating,double _age) {
		sid    = _sid;
		sname  = _sname;
		rating = _rating;
		age    = _age;
	}
}

//Define the Boat schema
class Boats {
	public int    bid;
	public String bname;
	public String color;

	public Boats (int _bid, String _bname, String _color) {
		bid   = _bid;
		bname = _bname;
		color = _color;
	}
}

//Define the Reserves schema
class Reserves {
	public int    sid;
	public int    bid;
	public String date;

	public Reserves (int _sid, int _bid, String _date) {
		sid  = _sid;
		bid  = _bid;
		date = _date;
	}
}


class Rule {
	public String outerRule;
	public String innerRule;
	public int ruleType;

	public static int RULE_TYPE_PARENT_CHILD = 0;
	public static int RULE_TYPE_ANCESTRAL_DESCENDENT = 1;

	public Rule(String _outerRule, String _innerRule, int _ruleType) {
		this.outerRule = _outerRule;
		this.innerRule = _innerRule;
		this.ruleType = _ruleType;
	}
}

class JoinsDriver implements GlobalConst {

	private boolean OK = true;
	private boolean FAIL = false;
	private Vector sailors;
	private Vector boats;
	private Vector reserves;
	private Vector nodes;
	/** Constructor
	 */
	public JoinsDriver() {

		// build Sailor, Boats, Reserves table
		sailors = new Vector();
		boats = new Vector();
		reserves = new Vector();
		nodes = new Vector();

		sailors.addElement(new Sailor(53, "Bob Holloway", 9, 53.6));
		sailors.addElement(new Sailor(54, "Susan Horowitz", 1, 34.2));
		sailors.addElement(new Sailor(57, "Yannis Ioannidis", 8, 40.2));
		sailors.addElement(new Sailor(59, "Deborah Joseph", 10, 39.8));
		sailors.addElement(new Sailor(61, "Landwebber", 8, 56.7));
		sailors.addElement(new Sailor(63, "James Larus", 9, 30.3));
		sailors.addElement(new Sailor(64, "Barton Miller", 5, 43.7));
		sailors.addElement(new Sailor(67, "David Parter", 1, 99.9));
		sailors.addElement(new Sailor(69, "Raghu Ramakrishnan", 9, 37.1));
		sailors.addElement(new Sailor(71, "Guri Sohi", 10, 42.1));
		sailors.addElement(new Sailor(73, "Prasoon Tiwari", 8, 39.2));
		sailors.addElement(new Sailor(39, "Anne Condon", 3, 30.3));
		sailors.addElement(new Sailor(47, "Charles Fischer", 6, 46.3));
		sailors.addElement(new Sailor(49, "James Goodman", 4, 50.3));
		sailors.addElement(new Sailor(50, "Mark Hill", 5, 35.2));
		sailors.addElement(new Sailor(75, "Mary Vernon", 7, 43.1));
		sailors.addElement(new Sailor(79, "David Wood", 3, 39.2));
		sailors.addElement(new Sailor(84, "Mark Smucker", 9, 25.3));
		sailors.addElement(new Sailor(87, "Martin Reames", 10, 24.1));
		sailors.addElement(new Sailor(10, "Mike Carey", 9, 40.3));
		sailors.addElement(new Sailor(21, "David Dewitt", 10, 47.2));
		sailors.addElement(new Sailor(29, "Tom Reps", 7, 39.1));
		sailors.addElement(new Sailor(31, "Jeff Naughton", 5, 35.0));
		sailors.addElement(new Sailor(31, "Jeff Naughton", 5, 35.0));
		sailors.addElement(new Sailor(35, "Miron Livny", 7, 37.6));
		sailors.addElement(new Sailor(37, "Marv Solomon", 10, 48.9));

		boats.addElement(new Boats(1, "Onion", "white"));
		boats.addElement(new Boats(2, "Buckey", "red"));
		boats.addElement(new Boats(3, "Enterprise", "blue"));
		boats.addElement(new Boats(4, "Voyager", "green"));
		boats.addElement(new Boats(5, "Wisconsin", "red"));

		reserves.addElement(new Reserves(10, 1, "05/10/95"));
		reserves.addElement(new Reserves(21, 1, "05/11/95"));
		reserves.addElement(new Reserves(10, 2, "05/11/95"));
		reserves.addElement(new Reserves(31, 1, "05/12/95"));
		reserves.addElement(new Reserves(31, 1, "05/12/96"));
		reserves.addElement(new Reserves(10, 3, "05/13/95"));
		reserves.addElement(new Reserves(69, 4, "05/12/95"));
		reserves.addElement(new Reserves(69, 5, "05/14/95"));
		reserves.addElement(new Reserves(21, 5, "05/16/95"));
		reserves.addElement(new Reserves(57, 2, "05/10/95"));
		reserves.addElement(new Reserves(35, 3, "05/15/95"));

		nodes.addElement(new NodeTable("A", new IntervalType(1, 18, 1)));
		nodes.addElement(new NodeTable("B", new IntervalType(2, 7, 2)));
		nodes.addElement(new NodeTable("B", new IntervalType(8, 15, 2)));
		nodes.addElement(new NodeTable("B", new IntervalType(16, 17, 2)));
		nodes.addElement(new NodeTable("E", new IntervalType(3, 6, 3)));
		nodes.addElement(new NodeTable("E", new IntervalType(9, 12, 3)));
		nodes.addElement(new NodeTable("D", new IntervalType(13, 14, 3)));
		nodes.addElement(new NodeTable("F", new IntervalType(4, 5, 4)));
		nodes.addElement(new NodeTable("F", new IntervalType(10, 11, 4)));

		boolean status = OK;
		int numsailors = 26;
		int numsailors_attrs = 4;
		int numreserves = 11;
		int numreserves_attrs = 3;
		int numboats = 5;
		int numboats_attrs = 3;
		int numnodes = 9;
		int numnodes_attrs = 2;

		String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
		String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

		String remove_cmd = "/bin/rm -rf ";
		String remove_logcmd = remove_cmd + logpath;
		String remove_dbcmd = remove_cmd + dbpath;
		String remove_joincmd = remove_cmd + dbpath;

		try {
			Runtime.getRuntime().exec(remove_logcmd);
			Runtime.getRuntime().exec(remove_dbcmd);
			Runtime.getRuntime().exec(remove_joincmd);
		} catch (IOException e) {
			System.err.println("" + e);
		}

		/*
		 * ExtendedSystemDefs extSysDef = new ExtendedSystemDefs(
		 * "/tmp/minibase.jointestdb", "/tmp/joinlog", 1000,500,200,"Clock");
		 */

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		//creating the node table relation
		AttrType[] Ntypes = new AttrType[2];
		Ntypes[0] = new AttrType(AttrType.attrInterval);
		Ntypes[1] = new AttrType(AttrType.attrString);

		short[] Nsizes = new short[1];
		Nsizes[0] = 15; // first elt. is 30

		Tuple t = new Tuple();
		try {
			t.setHdr((short) 2, Ntypes, Nsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		int size = t.size();

		// inserting the tuple into file "sailors"
		RID rid;
		Heapfile f = null;
		try {
			f = new Heapfile("nodes.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 2, Ntypes, Nsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < numnodes; i++) {
			try {
				t.setIntervalFld(1, ((NodeTable) nodes.elementAt(i)).interval);
				t.setStrFld(2, ((NodeTable) nodes.elementAt(i)).nodename);
			} catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
		}
		if (status != OK) {
			// bail out
			System.err.println("*** Error creating relation for n");
			Runtime.getRuntime().exit(1);
		}

		// creating the sailors relation
		AttrType[] Stypes = new AttrType[4];
		Stypes[0] = new AttrType(AttrType.attrInteger);
		Stypes[1] = new AttrType(AttrType.attrString);
		Stypes[2] = new AttrType(AttrType.attrInteger);
		Stypes[3] = new AttrType(AttrType.attrReal);

		// SOS
		short[] Ssizes = new short[1];
		Ssizes[0] = 30; // first elt. is 30

		t = new Tuple();
		try {
			t.setHdr((short) 4, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		size = t.size();

		// inserting the tuple into file "sailors"
		try {
			f = new Heapfile("sailors.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 4, Stypes, Ssizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < numsailors; i++) {
			try {
				t.setIntFld(1, ((Sailor) sailors.elementAt(i)).sid);
				t.setStrFld(2, ((Sailor) sailors.elementAt(i)).sname);
				t.setIntFld(3, ((Sailor) sailors.elementAt(i)).rating);
				t.setFloFld(4, (float) ((Sailor) sailors.elementAt(i)).age);
			} catch (Exception e) {
				System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
		}
		if (status != OK) {
			// bail out
			System.err.println("*** Error creating relation for sailors");
			Runtime.getRuntime().exit(1);
		}

		// creating the boats relation
		AttrType[] Btypes = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString), };

		short[] Bsizes = new short[2];
		Bsizes[0] = 30;
		Bsizes[1] = 20;
		t = new Tuple();
		try {
			t.setHdr((short) 3, Btypes, Bsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		size = t.size();

		// inserting the tuple into file "boats"
		// RID rid;
		f = null;
		try {
			f = new Heapfile("boats.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 3, Btypes, Bsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < numboats; i++) {
			try {
				t.setIntFld(1, ((Boats) boats.elementAt(i)).bid);
				t.setStrFld(2, ((Boats) boats.elementAt(i)).bname);
				t.setStrFld(3, ((Boats) boats.elementAt(i)).color);
			} catch (Exception e) {
				System.err.println("*** error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
		}
		if (status != OK) {
			// bail out
			System.err.println("*** Error creating relation for boats");
			Runtime.getRuntime().exit(1);
		}

		// creating the boats relation
		AttrType[] Rtypes = new AttrType[3];
		Rtypes[0] = new AttrType(AttrType.attrInteger);
		Rtypes[1] = new AttrType(AttrType.attrInteger);
		Rtypes[2] = new AttrType(AttrType.attrString);

		short[] Rsizes = new short[1];
		Rsizes[0] = 15;
		t = new Tuple();
		try {
			t.setHdr((short) 3, Rtypes, Rsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		size = t.size();

		// inserting the tuple into file "boats"
		// RID rid;
		f = null;
		try {
			f = new Heapfile("reserves.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}

		t = new Tuple(size);
		try {
			t.setHdr((short) 3, Rtypes, Rsizes);
		} catch (Exception e) {
			System.err.println("*** error in Tuple.setHdr() ***");
			status = FAIL;
			e.printStackTrace();
		}

		for (int i = 0; i < numreserves; i++) {
			try {
				t.setIntFld(1, ((Reserves) reserves.elementAt(i)).sid);
				t.setIntFld(2, ((Reserves) reserves.elementAt(i)).bid);
				t.setStrFld(3, ((Reserves) reserves.elementAt(i)).date);

			} catch (Exception e) {
				System.err.println("*** error in Tuple.setStrFld() ***");
				status = FAIL;
				e.printStackTrace();
			}

			try {
				rid = f.insertRecord(t.returnTupleByteArray());
			} catch (Exception e) {
				System.err.println("*** error in Heapfile.insertRecord() ***");
				status = FAIL;
				e.printStackTrace();
			}
		}
		if (status != OK) {
			// bail out
			System.err.println("*** Error creating relation for reserves");
			Runtime.getRuntime().exit(1);
		}

	}

	public boolean runTests() {

		Disclaimer();
		ProjectSM();


		System.out.print ("Finished joins testing"+"\n");


		return true;
	}

	private void Query1_CondExpr(CondExpr[] expr) {

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[1].next  = null;
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrInteger);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
		expr[1].operand2.integer = 1;

		expr[2] = null;
	}

	private void setConditions(CondExpr[] outFilter, Rule rule, int offset, boolean isFirstRule) {

		int outerIntervalColNo = offset;
		int outerTagNameColNo ;
		// The original schema is Interval - Tag Name. Since the projection is Tag Name - Interval
		// the column numbers are swapped. Since no projection has been applied to the first rule,
		// it retains the table schema.
		if(isFirstRule) {
			outerTagNameColNo = offset + 1;
		} else {
			outerTagNameColNo = offset - 1;
		}

		//Join Condition
		outFilter[0].next = null;
		outFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		outFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outerIntervalColNo);
		outFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		if (rule.ruleType == Rule.RULE_TYPE_PARENT_CHILD) {
			outFilter[0].flag = 2;
		} else {
			outFilter[0].flag = 1;
		}

		//Outer table comparison. For eg: If rule is A B PC, this condition will return
		//results for outer table where tag name equals to A.
		outFilter[1].next = null;
		outFilter[1].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outerTagNameColNo);
		outFilter[1].type2 = new AttrType(AttrType.attrString);
		outFilter[1].operand2.string = rule.outerRule;

		//Inner table comparison.
		outFilter[2].next = null;
		outFilter[2].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[2].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[2].type2 = new AttrType(AttrType.attrString);
		outFilter[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		outFilter[2].operand2.string = rule.innerRule;

		outFilter[3] = null;
	}
	private void Query2_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr[1] = null;

		expr2[0].next  = null;
		expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);
		expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
		expr2[1].next = null;
		expr2[1].type1 = new AttrType(AttrType.attrSymbol);
		expr2[1].type2 = new AttrType(AttrType.attrString);
		expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
		expr2[1].operand2.string = "red";

		expr2[2] = null;
	}

	private void Query3_CondExpr(CondExpr[] expr) {

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
		expr[1] = null;
	}

	private CondExpr[] Query5_CondExpr() {
		CondExpr [] expr2 = new CondExpr[3];
		expr2[0] = new CondExpr();


		expr2[0].next  = null;
		expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);

		expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),1);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);

		expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr2[1] = new CondExpr();
		expr2[1].op   = new AttrOperator(AttrOperator.aopGT);
		expr2[1].next = null;
		expr2[1].type1 = new AttrType(AttrType.attrSymbol);

		expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
		expr2[1].type2 = new AttrType(AttrType.attrReal);
		expr2[1].operand2.real = (float)40.0;


		expr2[1].next = new CondExpr();
		expr2[1].next.op   = new AttrOperator(AttrOperator.aopLT);
		expr2[1].next.next = null;
		expr2[1].next.type1 = new AttrType(AttrType.attrSymbol); // rating
		expr2[1].next.operand1.symbol = new FldSpec ( new RelSpec(RelSpec.outer),3);
		expr2[1].next.type2 = new AttrType(AttrType.attrInteger);
		expr2[1].next.operand2.integer = 7;

		expr2[2] = null;
		return expr2;
	}

	private void Query6_CondExpr(CondExpr[] expr, CondExpr[] expr2) {

		expr[0].next  = null;
		expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);

		expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);

		expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr[1].next  = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopGT);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);

		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
		expr[1].type2 = new AttrType(AttrType.attrInteger);
		expr[1].operand2.integer = 7;

		expr[2] = null;

		expr2[0].next  = null;
		expr2[0].op    = new AttrOperator(AttrOperator.aopEQ);
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);

		expr2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);

		expr2[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

		expr2[1].next = null;
		expr2[1].op   = new AttrOperator(AttrOperator.aopEQ);
		expr2[1].type1 = new AttrType(AttrType.attrSymbol);

		expr2[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
		expr2[1].type2 = new AttrType(AttrType.attrString);
		expr2[1].operand2.string = "red";

		expr2[2] = null;
	}


	private void Proj_CondExpr(CondExpr[] expr) {
		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		expr[0].flag = 1;

		expr[1].next = null;
		expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
		expr[1].type2 = new AttrType(AttrType.attrString);
		expr[1].operand2.string = "A";

		CondExpr orExpr = new CondExpr();
		expr[2].next = orExpr;
		expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
		expr[2].type1 = new AttrType(AttrType.attrSymbol);
		expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
		expr[2].type2 = new AttrType(AttrType.attrString);
		expr[2].operand2.string = "E";


		orExpr.next = null;
		orExpr.op    = new AttrOperator(AttrOperator.aopEQ);
		orExpr.type1 = new AttrType(AttrType.attrSymbol);
		orExpr.operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
		orExpr.type2 = new AttrType(AttrType.attrString);
		orExpr.operand2.string = "B";

		expr[3] = null;
	}

	private void Project_CondExpr(CondExpr[] expr, List<Rule> rules, int nodeNumber) {
		String outerNode = rules.get(0).outerRule;
		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1 + 2 * (nodeNumber - 1));
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		expr[0].flag = 1;

		expr[1].next = null;
		expr[1].op = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2 + 2 * (nodeNumber - 1));
		expr[1].type2 = new AttrType(AttrType.attrString);
		expr[1].operand2.string = outerNode;

		boolean first = true;
		CondExpr prevExp = null;
		for (Rule rule : rules) {
			if (first) {
				expr[2].op = new AttrOperator(AttrOperator.aopEQ);
				expr[2].type1 = new AttrType(AttrType.attrSymbol);
				expr[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
				expr[2].type2 = new AttrType(AttrType.attrString);
				expr[2].operand2.string = rule.innerRule;
				if (rule.ruleType == Rule.RULE_TYPE_PARENT_CHILD) {
					expr[2].flag = 2;
				}
				first = false;
				prevExp = expr[2];
			} else {
				CondExpr nextNode = new CondExpr();
				prevExp.next = nextNode;
				nextNode.op = new AttrOperator(AttrOperator.aopEQ);
				nextNode.type1 = new AttrType(AttrType.attrSymbol);
				nextNode.operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
				nextNode.type2 = new AttrType(AttrType.attrString);
				nextNode.operand2.string = rule.innerRule;
				if (rule.ruleType == Rule.RULE_TYPE_PARENT_CHILD) {
					nextNode.flag = 2;
				}
				prevExp = nextNode;
			}
		}

		expr[3] = null;
	}

	private void Project2_CondExpr(CondExpr[] expr, Rule rule) {

		String outerNode = rule.outerRule;
		expr[0].next = null;
		expr[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrSymbol);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		if (rule.ruleType == Rule.RULE_TYPE_PARENT_CHILD) {
			expr[0].flag = 2;
		} else {
			expr[0].flag = 1;
		}

		expr[1].next = null;
		expr[1].op = new AttrOperator(AttrOperator.aopEQ);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		expr[1].type2 = new AttrType(AttrType.attrString);
		expr[1].operand2.string = outerNode;

		expr[2].next = null;
		expr[2].op = new AttrOperator(AttrOperator.aopEQ);
		expr[2].type1 = new AttrType(AttrType.attrSymbol);
		expr[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		expr[2].type2 = new AttrType(AttrType.attrString);
		expr[2].operand2.string = rule.innerRule;

		expr[3] = null;
	}

	private void Project3_CondExpr(CondExpr[] OutFilter, Rule rule, int offset) {

		String outerNode = rule.outerRule;

		OutFilter[0].next = null;
		OutFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		OutFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		OutFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		OutFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset);
		OutFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		if (rule.ruleType == Rule.RULE_TYPE_PARENT_CHILD) {
			OutFilter[0].flag = 2;
		} else {
			OutFilter[0].flag = 1;
		}

		OutFilter[1].next = null;
		OutFilter[1].op = new AttrOperator(AttrOperator.aopEQ);
		OutFilter[1].type1 = new AttrType(AttrType.attrSymbol);
		OutFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset - 1);
		OutFilter[1].type2 = new AttrType(AttrType.attrString);
		OutFilter[1].operand2.string = outerNode;

		OutFilter[2].next = null;
		OutFilter[2].op = new AttrOperator(AttrOperator.aopEQ);
		OutFilter[2].type1 = new AttrType(AttrType.attrSymbol);
		OutFilter[2].type2 = new AttrType(AttrType.attrString);
		OutFilter[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		OutFilter[2].operand2.string = rule.innerRule;

		OutFilter[3] = null;
	}

	private void Project4_CondExpr(CondExpr[] OutFilter, Rule rule, int offset) {

		String outerNode = rule.outerRule;

		OutFilter[0].next = null;
		OutFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		OutFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		OutFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		OutFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset);
		OutFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		OutFilter[0].flag =0;

	}

	private void populateNodeOffsetMap(Map<String, Integer> offsetMap, String nodeName, Integer nodeNumber) {
		offsetMap.put(nodeName, 2*nodeNumber);
		nodeNumber = nodeNumber + 1;
	}

	public void Query1() {

		System.out.print("**********************Query1 strating *********************\n");
		boolean status = OK;

		// Sailors, Boats, Reserves Queries.
		System.out.print ("Query: Find the names of sailors who have reserved "
				+ "boat number 1.\n"
				+ "       and print out the date of reservation.\n\n"
				+ "  SELECT S.sname, R.date\n"
				+ "  FROM   Sailors S, Reserves R\n"
				+ "  WHERE  S.sid = R.sid AND R.bid = 1\n\n");

		System.out.print ("\n(Tests FileScan, Projection, and Sort-Merge Join)\n");

		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();

		Query1_CondExpr(outFilter);

		Tuple t = new Tuple();

		AttrType [] Stypes = new AttrType[4];
		Stypes[0] = new AttrType (AttrType.attrInteger);
		Stypes[1] = new AttrType (AttrType.attrString);
		Stypes[2] = new AttrType (AttrType.attrInteger);
		Stypes[3] = new AttrType (AttrType.attrReal);

		//SOS
		short [] Ssizes = new short[1];
		Ssizes[0] = 30; //first elt. is 30

		FldSpec [] Sprojection = new FldSpec[4];
		Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Sprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Sprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
		Sprojection[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);

		CondExpr [] selects = new CondExpr [1];
		selects = null;


		FileScan am = null;
		try {
			am  = new FileScan("sailors.in", Stypes, Ssizes,
					(short)4, (short)4,
					Sprojection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
		}

		AttrType [] Rtypes = new AttrType[3];
		Rtypes[0] = new AttrType (AttrType.attrInteger);
		Rtypes[1] = new AttrType (AttrType.attrInteger);
		Rtypes[2] = new AttrType (AttrType.attrString);

		short [] Rsizes = new short[1];
		Rsizes[0] = 15;
		FldSpec [] Rprojection = new FldSpec[3];
		Rprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
		Rprojection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		Rprojection[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);

		FileScan am2 = null;
		try {
			am2 = new FileScan("reserves.in", Rtypes, Rsizes,
					(short)3, (short) 3,
					Rprojection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}


		FldSpec [] proj_list = new FldSpec[2];
		proj_list[0] = new FldSpec(new RelSpec(RelSpec.outer), 2);
		proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

		AttrType [] jtype = new AttrType[2];
		jtype[0] = new AttrType (AttrType.attrString);
		jtype[1] = new AttrType (AttrType.attrString);

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		SortMerge sm = null;
		try {
			sm = new SortMerge(Stypes, 4, Ssizes,
					Rtypes, 3, Rsizes,
					1, 4,
					1, 4,
					10,
					am, am2,
					false, false, ascending,
					outFilter, proj_list, 2);
		}
		catch (Exception e) {
			System.err.println("*** join error in SortMerge constructor ***");
			status = FAIL;
			System.err.println (""+e);
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}



		QueryCheck qcheck1 = new QueryCheck(1);


		t = null;

		try {
			while ((t = sm.get_next()) != null) {
				t.print(jtype);

				qcheck1.Check(t);
			}
		}
		catch (Exception e) {
			System.err.println (""+e);
			e.printStackTrace();
			status = FAIL;
		}
		if (status != OK) {
			//bail out
			System.err.println ("*** Error in get next tuple ");
			Runtime.getRuntime().exit(1);
		}

		qcheck1.report(1);
		try {
			sm.close();
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		System.out.println ("\n");
		if (status != OK) {
			//bail out
			System.err.println ("*** Error in closing ");
			Runtime.getRuntime().exit(1);
		}
	}

	public void Query2() {
		System.out.print("**********************Query2 strating *********************\n");
		boolean status = OK;

		// Sailors, Boats, Reserves Queries.
		System.out.print("Query: Find the names of sailors who have reserved " + "a red boat\n"
				+ "       and return them in alphabetical order.\n\n" + "  SELECT   S.sname\n"
				+ "  FROM     Sailors S, Boats B, Reserves R\n"
				+ "  WHERE    S.sid = R.sid AND R.bid = B.bid AND B.color = 'red'\n" + "  ORDER BY S.sname\n"
				+ "Plan used:\n" + " Sort (Pi(sname) (Sigma(B.color='red')  "
				+ "|><|  Pi(sname, bid) (S  |><|  R)))\n\n"
				+ "(Tests File scan, Index scan ,Projection,  index selection,\n "
				+ "sort and simple nested-loop join.)\n\n");

		// Build Index first
		IndexType b_index = new IndexType(IndexType.B_Index);

		// ExtendedSystemDefs.MINIBASE_CATALOGPTR.addIndex("sailors.in", "sid", b_index,
		// 1);
		// }
		// catch (Exception e) {
		// e.printStackTrace();
		// System.err.print ("Failure to add index.\n");
		// Runtime.getRuntime().exit(1);
		// }

		CondExpr[] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();

		CondExpr[] outFilter2 = new CondExpr[3];
		outFilter2[0] = new CondExpr();
		outFilter2[1] = new CondExpr();
		outFilter2[2] = new CondExpr();

		Query2_CondExpr(outFilter, outFilter2);
		Tuple t = new Tuple();
		t = null;

		AttrType[] Stypes = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrReal) };

		AttrType[] Stypes2 = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString), };

		short[] Ssizes = new short[1];
		Ssizes[0] = 30;
		AttrType[] Rtypes = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), };

		short[] Rsizes = new short[1];
		Rsizes[0] = 15;
		AttrType[] Btypes = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString), };

		short[] Bsizes = new short[2];
		Bsizes[0] = 30;
		Bsizes[1] = 20;
		AttrType[] Jtypes = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), };

		short[] Jsizes = new short[1];
		Jsizes[0] = 30;
		AttrType[] JJtype = { new AttrType(AttrType.attrString), };

		short[] JJsize = new short[1];
		JJsize[0] = 30;
		FldSpec[] proj1 = { new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 2) }; // S.sname,
		// R.bid

		FldSpec[] proj2 = { new FldSpec(new RelSpec(RelSpec.outer), 1) };

		FldSpec[] Sprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				// new FldSpec(new RelSpec(RelSpec.outer), 3),
				// new FldSpec(new RelSpec(RelSpec.outer), 4)
		};

		CondExpr[] selects = new CondExpr[1];
		selects[0] = null;

		// IndexType b_index = new IndexType(IndexType.B_Index);
		iterator.Iterator am = null;

		// _______________________________________________________________
		// *******************create an scan on the heapfile**************
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// create a tuple of appropriate size
		Tuple tt = new Tuple();
		try {
			tt.setHdr((short) 4, Stypes, Ssizes);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		int sizett = tt.size();
		tt = new Tuple(sizett);
		try {
			tt.setHdr((short) 4, Stypes, Ssizes);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		Heapfile f = null;
		try {
			f = new Heapfile("sailors.in");
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		Scan scan = null;

		try {
			scan = new Scan(f);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// create the index file
		BTreeFile btf = null;
		try {
			btf = new BTreeFile("BTreeIndex", AttrType.attrInteger, 4, 1);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		RID rid = new RID();
		int key = 0;
		Tuple temp = null;

		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		while (temp != null) {
			tt.tupleCopy(temp);

			try {
				key = tt.getIntFld(1);
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			try {
				btf.insert(new IntegerKey(key), rid);
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}

			try {
				temp = scan.getNext(rid);
			} catch (Exception e) {
				status = FAIL;
				e.printStackTrace();
			}
		}

		// close the file scan
		scan.closescan();

		// _______________________________________________________________
		// *******************close an scan on the heapfile**************
		// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		System.out.print("After Building btree index on sailors.sid.\n\n");
		try {
			am = new IndexScan(b_index, "sailors.in", "BTreeIndex", Stypes, Ssizes, 4, 2, Sprojection, null, 1, false);
		}

		catch (Exception e) {
			System.err.println("*** Error creating scan for Index scan");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		NestedLoopsJoins nlj = null;
		try {
			nlj = new NestedLoopsJoins(Stypes2, 2, Ssizes, Rtypes, 3, Rsizes, 10, am, "reserves.in", outFilter, null,
					proj1, 2);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		NestedLoopsJoins nlj2 = null;
		try {
			nlj2 = new NestedLoopsJoins(Jtypes, 2, Jsizes, Btypes, 3, Bsizes, 10, nlj, "boats.in", outFilter2, null,
					proj2, 1);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		Sort sort_names = null;
		try {
			sort_names = new Sort(JJtype, (short) 1, JJsize, (iterator.Iterator) nlj2, 1, ascending, JJsize[0], 10);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		QueryCheck qcheck2 = new QueryCheck(2);

		t = null;
		try {

			while ((t = sort_names.get_next()) != null) {
				t.print(JJtype);
				qcheck2.Check(t);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		qcheck2.report(2);

		System.out.println("\n");
		try {
			sort_names.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			// bail out

			Runtime.getRuntime().exit(1);
		}
	}


	public void Query3() {
		System.out.print("**********************Query3 strating *********************\n");
		boolean status = OK;

		// Sailors, Boats, Reserves Queries.

		System.out.print
				( "Query: Find the names of sailors who have reserved a boat.\n\n"
						+ "  SELECT S.sname\n"
						+ "  FROM   Sailors S, Reserves R\n"
						+ "  WHERE  S.sid = R.sid\n\n"
						+ "(Tests FileScan, Projection, and SortMerge Join.)\n\n");

		CondExpr [] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();

		Query3_CondExpr(outFilter);

		Tuple t = new Tuple();
		t = null;

		AttrType Stypes[] = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrReal)
		};
		short []   Ssizes = new short[1];
		Ssizes[0] = 30;

		AttrType [] Rtypes = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString),
		};
		short  []  Rsizes = new short[1];
		Rsizes[0] =15;

		FldSpec [] Sprojection = {
				new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 3),
				new FldSpec(new RelSpec(RelSpec.outer), 4)
		};

		CondExpr[] selects = new CondExpr [1];
		selects = null;

		iterator.Iterator am = null;
		try {
			am  = new FileScan("sailors.in", Stypes, Ssizes,
					(short)4, (short) 4,
					Sprojection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
		}

		FldSpec [] Rprojection = {
				new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 3)
		};

		iterator.Iterator am2 = null;
		try {
			am2 = new FileScan("reserves.in", Rtypes, Rsizes,
					(short)3, (short)3,
					Rprojection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}

		FldSpec [] proj_list = {
				new FldSpec(new RelSpec(RelSpec.outer), 2)
		};

		AttrType [] jtype     = { new AttrType(AttrType.attrString) };

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		SortMerge sm = null;
		try {
			sm = new SortMerge(Stypes, 4, Ssizes,
					Rtypes, 3, Rsizes,
					1, 4,
					1, 4,
					10,
					am, am2,
					false, false, ascending,
					outFilter, proj_list, 1);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}

		QueryCheck qcheck3 = new QueryCheck(3);


		t = null;

		try {
			while ((t = sm.get_next()) != null) {
				t.print(jtype);
				qcheck3.Check(t);
			}
		}
		catch (Exception e) {
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}


		qcheck3.report(3);

		System.out.println ("\n");
		try {
			sm.close();
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
		}
	}

	public void Query4() {
		System.out.print("**********************Query4 strating *********************\n");
		boolean status = OK;

		// Sailors, Boats, Reserves Queries.

		System.out.print
				("Query: Find the names of sailors who have reserved a boat\n"
						+ "       and print each name once.\n\n"
						+ "  SELECT DISTINCT S.sname\n"
						+ "  FROM   Sailors S, Reserves R\n"
						+ "  WHERE  S.sid = R.sid\n\n"
						+ "(Tests FileScan, Projection, Sort-Merge Join and "
						+ "Duplication elimination.)\n\n");

		CondExpr [] outFilter = new CondExpr[2];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();

		Query3_CondExpr(outFilter);

		Tuple t = new Tuple();
		t = null;

		AttrType Stypes[] = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrReal)
		};
		short []   Ssizes = new short[1];
		Ssizes[0] = 30;

		AttrType [] Rtypes = {
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString),
		};
		short  []  Rsizes = new short[1];
		Rsizes[0] =15;

		FldSpec [] Sprojection = {
				new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 3),
				new FldSpec(new RelSpec(RelSpec.outer), 4)
		};

		CondExpr[] selects = new CondExpr [1];
		selects = null;

		iterator.Iterator am = null;
		try {
			am  = new FileScan("sailors.in", Stypes, Ssizes,
					(short)4, (short) 4,
					Sprojection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
		}

		FldSpec [] Rprojection = {
				new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 3)
		};

		iterator.Iterator am2 = null;
		try {
			am2 = new FileScan("reserves.in", Rtypes, Rsizes,
					(short)3, (short)3,
					Rprojection, null);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}

		FldSpec [] proj_list = {
				new FldSpec(new RelSpec(RelSpec.outer), 2)
		};

		AttrType [] jtype     = { new AttrType(AttrType.attrString) };

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		SortMerge sm = null;
		short  []  jsizes    = new short[1];
		jsizes[0] = 30;
		try {
			sm = new SortMerge(Stypes, 4, Ssizes,
					Rtypes, 3, Rsizes,
					1, 4,
					1, 4,
					10,
					am, am2,
					false, false, ascending,
					outFilter, proj_list, 1);
		}
		catch (Exception e) {
			status = FAIL;
			System.err.println (""+e);
		}

		if (status != OK) {
			//bail out
			System.err.println ("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}



		DuplElim ed = null;
		try {
			ed = new DuplElim(jtype, (short)1, jsizes, sm, 10, false);
		}
		catch (Exception e) {
			System.err.println (""+e);
			Runtime.getRuntime().exit(1);
		}

		QueryCheck qcheck4 = new QueryCheck(4);


		t = null;

		try {
			while ((t = ed.get_next()) != null) {
				t.print(jtype);
				qcheck4.Check(t);
			}
		}
		catch (Exception e) {
			System.err.println (""+e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		qcheck4.report(4);
		try {
			ed.close();
		}
		catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		System.out.println ("\n");
		if (status != OK) {
			//bail out
			System.err.println ("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
		}
	}

	public void Query5() {
		System.out.print("**********************Query5 strating *********************\n");
		boolean status = OK;
		// Sailors, Boats, Reserves Queries.

		System.out.print("Query: Find the names of old sailors or sailors with "
				+ "a rating less\n       than 7, who have reserved a boat, "
				+ "(perhaps to increase the\n       amount they have to " + "pay to make a reservation).\n\n"
				+ "  SELECT S.sname, S.rating, S.age\n" + "  FROM   Sailors S, Reserves R\n"
				+ "  WHERE  S.sid = R.sid and (S.age > 40 || S.rating < 7)\n\n"
				+ "(Tests FileScan, Multiple Selection, Projection, " + "and Sort-Merge Join.)\n\n");

		CondExpr[] outFilter;
		outFilter = Query5_CondExpr();

		Tuple t = new Tuple();
		t = null;

		AttrType Stypes[] = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrReal) };
		short[] Ssizes = new short[1];
		Ssizes[0] = 30;

		AttrType[] Rtypes = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), };
		short[] Rsizes = new short[1];
		Rsizes[0] = 15;

		FldSpec[] Sprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
				new FldSpec(new RelSpec(RelSpec.outer), 4) };

		CondExpr[] selects = new CondExpr[1];
		selects[0] = null;

		FldSpec[] proj_list = { new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
				new FldSpec(new RelSpec(RelSpec.outer), 4) };

		FldSpec[] Rprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3) };

		AttrType[] jtype = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrReal) };

		iterator.Iterator am = null;
		try {
			am = new FileScan("sailors.in", Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
		}

		iterator.Iterator am2 = null;
		try {
			am2 = new FileScan("reserves.in", Rtypes, Rsizes, (short) 3, (short) 3, Rprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error setting up scan for reserves");
			Runtime.getRuntime().exit(1);
		}

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		SortMerge sm = null;
		try {
			sm = new SortMerge(Stypes, 4, Ssizes, Rtypes, 3, Rsizes, 1, 4, 1, 4, 10, am, am2, false, false, ascending,
					outFilter, proj_list, 3);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
		}

		if (status != OK) {
			// bail out
			System.err.println("*** Error constructing SortMerge");
			Runtime.getRuntime().exit(1);
		}

		QueryCheck qcheck5 = new QueryCheck(5);
		// Tuple t = new Tuple();
		t = null;

		try {
			while ((t = sm.get_next()) != null) {
				t.print(jtype);
				qcheck5.Check(t);
			}
		} catch (Exception e) {
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		qcheck5.report(5);
		try {
			sm.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}
		System.out.println("\n");
		if (status != OK) {
			// bail out
			System.err.println("*** Error close for sortmerge");
			Runtime.getRuntime().exit(1);
		}
	}

	public void Project3() {
		Rule rule1 = new Rule("A", "B", Rule.RULE_TYPE_PARENT_CHILD);
		Rule rule2 = new Rule("B", "E", Rule.RULE_TYPE_PARENT_CHILD);
		ArrayList<Rule> rules = new ArrayList<>();
		HashMap<String, Integer> nodeOffsetMap = new HashMap<>();
		rules.add(rule1);
		rules.add(rule2);
		int nodeNumber = 1;
		boolean status = OK;

		Iterator am = null;
		AttrType[] Ntypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
		short[] Nsizes = new short[1];
		Nsizes[0] = 1;

		FldSpec[] Nprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		try {
			am = new FileScan("nodes.in", Ntypes, Nsizes, (short) 2, (short) 2, Nprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}

		Rule firstRule = rules.get(0);
		populateNodeOffsetMap(nodeOffsetMap, firstRule.outerRule, nodeNumber);
		nodeNumber++;
		populateNodeOffsetMap(nodeOffsetMap, firstRule.innerRule, nodeNumber);
		nodeNumber++;

		CondExpr[] outFilter = new CondExpr[4];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();
		outFilter[3] = new CondExpr();

		Project2_CondExpr(outFilter, firstRule);
		rules.remove(0);

		FldSpec[] proj = { new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.innerRel), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 1) };
		NestedLoopsJoins inl = null;
		NestedLoopsJoins inl2 = null;
		try {
			inl = new NestedLoopsJoins(Ntypes, 2, Nsizes, Ntypes, 2, Nsizes, 10, am, "nodes.in", outFilter, null, proj,
					4);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		int ruleNumber = 2;
		for (Rule rule : rules) {
			if (!nodeOffsetMap.containsKey(rule.outerRule)) {
				//Technically, this should never happen.
				populateNodeOffsetMap(nodeOffsetMap, rule.outerRule, nodeNumber);
				nodeNumber++;
			}

			if (!nodeOffsetMap.containsKey(rule.innerRule)) {
				populateNodeOffsetMap(nodeOffsetMap, rule.innerRule, nodeNumber);
				nodeNumber++;
			}

			outFilter = new CondExpr[4];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();
			outFilter[2] = new CondExpr();
			outFilter[3] = new CondExpr();

			Project3_CondExpr(outFilter, rule, nodeOffsetMap.get(rule.outerRule));
			AttrType[] Ntypes2 = new AttrType[2 * ruleNumber];
			for (int i = 0; i < 2 * ruleNumber; i++) {
				if (i % 2 == 0) {
					Ntypes2[i] = new AttrType(AttrType.attrString);
				} else {
					Ntypes2[i] = new AttrType(AttrType.attrInterval);
				}
			}
			short[] Nsizes2 = new short[ruleNumber];
			for (int i = 0; i < ruleNumber; i++) {
				Nsizes2[i] = 1;
			}

			FldSpec[] proj2 = new FldSpec[2 * ruleNumber + 2];
			for (int i = 0; i < 2 * ruleNumber; i++) {
				proj2[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
			}
			proj2[2 * ruleNumber] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
			proj2[2 * ruleNumber + 1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

			try {
				inl2 = new NestedLoopsJoins(Ntypes2, 2 * ruleNumber, Nsizes2, Ntypes, 2, Nsizes, 10, inl, "nodes.in",
						outFilter, null, proj2, 2 * ruleNumber + 2);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			inl = inl2;
			ruleNumber++;
		}

		Tuple t = new Tuple();
		AttrType[] jtype = new AttrType[2 * ruleNumber + 2];

		for (int i = 0; i < 2 * ruleNumber; i++) {
			if (i % 2 == 0) {
				jtype[i] = new AttrType(AttrType.attrString);
			} else {
				jtype[i] = new AttrType(AttrType.attrInterval);
			}
		}

		try {
			while ((t = inl2.get_next()) != null) {
				t.print(jtype);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

	}

	public void ProjectSM() {
		Rule rule1 = new Rule("A", "B", Rule.RULE_TYPE_PARENT_CHILD);
		Rule rule2 = new Rule("A", "E", Rule.RULE_TYPE_ANCESTRAL_DESCENDENT);
		List<Rule> rules = new ArrayList<>();
		Map<String, Integer> nodeOffsetMap = new HashMap<>();
		rules.add(rule1);
		rules.add(rule2);
		//rules.add(rule3);
		int nodeNumber = 1;
		boolean status = OK;

		Iterator am = null;
		Iterator am1 = null;
		AttrType[] Ntypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
		short[] Nsizes = new short[1];
		Nsizes[0] = 1;

		FldSpec[] Nprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

		FldSpec[] proj = { new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.innerRel), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 1) };
		List<NestedLoopsJoins> listNLJ = new LinkedList<>();
		for(Rule rule:rules) {
			try {
				am = new FileScan("nodes.in", Ntypes, Nsizes, (short) 2, (short) 2, Nprojection, null);
			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
				e.printStackTrace();
			}
			NestedLoopsJoins inl = null;
			try {
				CondExpr[] outFilter = new CondExpr[4];
				outFilter[0] = new CondExpr();
				outFilter[1] = new CondExpr();
				outFilter[2] = new CondExpr();
				outFilter[3] = new CondExpr();
				setConditions(outFilter, rule, 1 , true);
				inl = new NestedLoopsJoins(Ntypes, 2, Nsizes, Ntypes, 2, Nsizes, 10, am, "nodes.in", outFilter, null, proj,
						4);
				listNLJ.add(inl);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
		}
		Rule prevRule = rules.get(0);
		populateNodeOffsetMap(nodeOffsetMap, prevRule.outerRule, nodeNumber);
		nodeNumber++;
		populateNodeOffsetMap(nodeOffsetMap, prevRule.innerRule, nodeNumber);
		nodeNumber++;
		Iterator prevSM = listNLJ.get(0);
		int index = 2;
		for(int x =1; x<listNLJ.size(); ++x) {
			if (!nodeOffsetMap.containsKey(rules.get(x).outerRule)) {
				//Technically, this should never happen.
				populateNodeOffsetMap(nodeOffsetMap, rules.get(x).outerRule, nodeNumber);
				nodeNumber++;
			}

			if (!nodeOffsetMap.containsKey(rules.get(x).innerRule)) {
				populateNodeOffsetMap(nodeOffsetMap, rules.get(x).innerRule, nodeNumber);
				nodeNumber++;
			}
			try {
				CondExpr[] outFilter = new CondExpr[2];
				outFilter[0] = new CondExpr();
				outFilter[1] = null;

				Project4_CondExpr(outFilter, rules.get(x), nodeOffsetMap.get(rules.get(x).outerRule));
				AttrType[] NtypesFix = { new AttrType(AttrType.attrString),
						new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInterval)};
				short[] NsizesFix = new short[2];
				NsizesFix[0] = 1;
				NsizesFix[1] = 1;
				AttrType[] Ntypes2 = new AttrType[2 * index];
				for (int i = 0; i < 2 * index; i++) {
					if (i % 2 == 0) {
						Ntypes2[i] = new AttrType(AttrType.attrString);
					} else {
						Ntypes2[i] = new AttrType(AttrType.attrInterval);
					}
				}
				short[] Nsizes2 = new short[index];
				for (int i = 0; i < index; i++) {
					Nsizes2[i] = 1;
				}
				FldSpec[] proj2 = new FldSpec[2 * index + 2];
				for (int i = 0; i < 2 * index; i++) {
					proj2[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
				}
				proj2[2 * index] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
				proj2[2 * index + 1] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
				SortMerge sm = new SortMerge(Ntypes2, index*2, Nsizes2,
						NtypesFix, 4, NsizesFix,
						nodeOffsetMap.get(rules.get(x).outerRule), 4,
						2, 4,
						10,
						prevSM, listNLJ.get(x),
						true, false, ascending,
						outFilter, proj2, 2*index + 2);
				index++;
				prevSM = sm;
			}
			catch (Exception e) {
				System.err.println("*** join error in SortMerge constructor ***");
				status = FAIL;
				System.err.println (""+e);
				e.printStackTrace();
			}
		}
		Tuple t = new Tuple();
		AttrType[] jtype = new AttrType[2 * index + 2];

		for (int i = 0; i < 2 * index; i++) {
			if (i % 2 == 0) {
				jtype[i] = new AttrType(AttrType.attrString);
			} else {
				jtype[i] = new AttrType(AttrType.attrInterval);
			}
		}

		try {
			while ((t = prevSM.get_next()) != null) {
				t.print(jtype);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

	}
/*
	public void Project2() {
		Rule rule1 = new Rule("A", "B", Rule.RULE_TYPE_PARENT_CHILD);
		Rule rule2 = new Rule("A", "E", Rule.RULE_TYPE_PARENT_CHILD);
		ArrayList<Rule> aRules = new ArrayList<>();
		aRules.add(rule1);
		aRules.add(rule2);
		int nodeIndex = 1;
		int ruleNumber = 1;
		boolean status = OK;
		boolean first = true;
		for (Rule rule : aRules) {
			Iterator am = null;
			AttrType[] Ntypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
			short[] Nsizes = new short[1];
			Nsizes[0] = 1;
			
			FldSpec[] Nprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.outer), 2) };
			try {
				am = new FileScan("nodes.in", Ntypes, Nsizes, (short) 2, (short) 2, Nprojection, null);
			} catch (Exception e) {
				status = FAIL;
				System.err.println("" + e);
				e.printStackTrace();
			}
			
			CondExpr[] outFilter = new CondExpr[4];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();
			outFilter[2] = new CondExpr();
			outFilter[3] = new CondExpr();
			
			CondExpr[] outFilter2 = new CondExpr[2];
			outFilter2[0] = new CondExpr();
			outFilter2[1] = new CondExpr();
			
			Project2_CondExpr(outFilter, outFilter2, rule , nodeIndex);
			FldSpec[] proj = { new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 1),
					new FldSpec(new RelSpec(RelSpec.innerRel), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 1) };
			NestedLoopsJoins inl = null;
			try {
				inl = new NestedLoopsJoins(Ntypes, 2, Nsizes, Ntypes, 2, Nsizes, 10, am,
						"nodes.in", outFilter, null, proj, 4);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			
			NestedLoopsJoins inl2 = null;
			if(!first) {
				AttrType[] Ntypes2 = new AttrType[2 * ruleNumber];
				for (int i = 0; i < 2 * ruleNumber; i++) {
					if (i % 2 == 0) {
						Ntypes2[i] = new AttrType(AttrType.attrInterval);
					} else {
						Ntypes2[i] = new AttrType(AttrType.attrString);
					}
				}
				short[] Nsizes2 = new short[ruleNumber];
				for (int i = 0; i < ruleNumber; i++) {
					Nsizes2[i] = 1;
				}
			}
		}

	}
	public void Project() {
		HashMap<String, ArrayList<Rule>> rulesMap = new HashMap<>();
		Rule rule1 = new Rule("A", "B", Rule.RULE_TYPE_PARENT_CHILD);
		Rule rule2 = new Rule("A", "E", Rule.RULE_TYPE_PARENT_CHILD);
		ArrayList<Rule> aRules = new ArrayList<>();
		aRules.add(rule1);
		//aRules.add(rule2);
		rulesMap.put("A", aRules);
//		Rule rule3 = new Rule("B", "C", Rule.RULE_TYPE_PARENT_CHILD);
//		ArrayList<Rule> bRules = new ArrayList<>();
//		bRules.add(rule3);
//		rulesMap.put("B", bRules);

		int nodeNumber = 1;
		Iterator am = null;
		AttrType[] Ntypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };

		short[] Nsizes = new short[1];
		Nsizes[0] = 1;
		boolean status = OK;
		FldSpec[] Nprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		try {
			am = new FileScan("nodes.in", Ntypes, Nsizes, (short) 2, (short) 2, Nprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}
		for (String node : rulesMap.keySet()) {

			AttrType[] ResultTypes = new AttrType[2 * nodeNumber];
			for (int i = 0; i < 2 * nodeNumber; i++) {
				if (i % 2 == 0) {
					ResultTypes[i] = new AttrType(AttrType.attrInterval);
				} else {
					ResultTypes[i] = new AttrType(AttrType.attrString);
				}
			}
			short[] ResultStringLength = new short[nodeNumber];
			for (int i = 0; i < nodeNumber; i++) {
				ResultStringLength[i] = 1;
			}
			// FileScan am = null;
			
			CondExpr[] outFilter = new CondExpr[4];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();
			outFilter[2] = new CondExpr();
			outFilter[3] = new CondExpr();
			// //CondExpr[] outFilter = new CondExpr[2 + rulesMap.get(node).size() + 1];
			// for (int i =0 ; i<=2 + 2*rulesMap.get(node).size(); i++) {
			// outFilter[i] = new CondExpr();
			// }
			Project_CondExpr(outFilter, rulesMap.get(node), nodeNumber);
			// FldSpec[] proj = { new FldSpec(new RelSpec(RelSpec.outer), 2), new
			// FldSpec(new RelSpec(RelSpec.outer), 1),
			// new FldSpec(new RelSpec(RelSpec.innerRel), 2), new FldSpec(new
			// RelSpec(RelSpec.innerRel), 1) }; // S.sname,

			FldSpec[] proj = new FldSpec[2 * nodeNumber + 2];
			for (int i = 0; i < 2 * nodeNumber; i++) {
				proj[i] = new FldSpec(new RelSpec(RelSpec.outer), 2 * nodeNumber - i);
			}
			proj[2 * nodeNumber] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
			proj[2 * nodeNumber + 1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

			NestedLoopsJoins inl = null;
			try {
				inl = new NestedLoopsJoins(ResultTypes, 2 * nodeNumber, ResultStringLength, Ntypes, 2, Nsizes, 10, am,
						"nodes.in", outFilter, null, proj, 4);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}

			Tuple t = new Tuple();
			// AttrType[] jtype = new AttrType[4];
			//
			// jtype[0] = new AttrType(AttrType.attrString);
			// jtype[1] = new AttrType(AttrType.attrInterval);
			// jtype[2] = new AttrType(AttrType.attrString);
			// jtype[3] = new AttrType(AttrType.attrInterval);
			//
			AttrType[] jtype = new AttrType[2 * nodeNumber + 2];

			for (int i = 0; i < 2 * nodeNumber + 2; i++) {
				if (i % 2 == 0) {
					jtype[i] = new AttrType(AttrType.attrString);
				} else {
					jtype[i] = new AttrType(AttrType.attrInterval);
				}
			}
			try {
				while ((t = inl.get_next()) != null) {
					t.print(jtype);
				}
			} catch (Exception e) {
				System.err.println("*** Error preparing for get_next tuple");
				System.err.println("" + e);
				Runtime.getRuntime().exit(1);
			}
			nodeNumber++;
			am = inl;
		}
	}


	public void Project_SM() {
		Rule rule1 = new Rule("A", "B", Rule.RULE_TYPE_PARENT_CHILD);
		Rule rule2 = new Rule("A", "E", Rule.RULE_TYPE_PARENT_CHILD);
		ArrayList<Rule> aRules = new ArrayList<>();
		aRules.add(rule1);
		aRules.add(rule2);
		boolean status = OK;

		Iterator am = null;
		AttrType[] Ntypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
		short[] Nsizes = new short[1];
		Nsizes[0] = 1;

		FldSpec[] Nprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		try {
			am = new FileScan("nodes.in", Ntypes, Nsizes, (short) 2, (short) 2, Nprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}

		Rule firstRule = aRules.get(0);

		CondExpr[] outFilter = new CondExpr[4];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();
		outFilter[3] = new CondExpr();

		CondExpr[] outFilter2 = new CondExpr[2];
		outFilter2[0] = new CondExpr();
		outFilter2[1] = new CondExpr();

		Project2_CondExpr(outFilter, outFilter2, firstRule, 1);
		String prevRuleNode = firstRule.outerRule;
		aRules.remove(0);

		FldSpec[] proj = { new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.innerRel), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 1) };
		NestedLoopsJoins inl = null;
		NestedLoopsJoins inl2 = null;
		try {
			inl = new NestedLoopsJoins(Ntypes, 2, Nsizes, Ntypes, 2, Nsizes, 10, am, "nodes.in", outFilter, null, proj,
					4);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		int nodeIndex = 1;
		int ruleNumber = 2;
		for (Rule rule : aRules) {
			if (rule.outerRule != prevRuleNode) {
				nodeIndex++;
			}

			outFilter = new CondExpr[3];
			outFilter[0] = new CondExpr();
			outFilter[1] = new CondExpr();
			outFilter[2] = new CondExpr();

			CondExpr[] RightFilter = new CondExpr[2];
			RightFilter[0] = new CondExpr();
			RightFilter[1] = new CondExpr();

			Project3_CondExpr(RightFilter, outFilter, rule, nodeIndex);
			AttrType[] Ntypes2 = new AttrType[2 * ruleNumber];
			for (int i = 0; i < 2 * ruleNumber; i++) {
				if (i % 2 == 0) {
					Ntypes2[i] = new AttrType(AttrType.attrInterval);
				} else {
					Ntypes2[i] = new AttrType(AttrType.attrString);
				}
			}
			short[] Nsizes2 = new short[ruleNumber];
			for (int i = 0; i < ruleNumber; i++) {
				Nsizes2[i] = 1;
			}

			FldSpec[] proj2 = new FldSpec[2 * ruleNumber + 2];
			for (int i = 0; i < 2 * ruleNumber; i++) {
				proj2[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
			}
			proj2[2 * ruleNumber] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
			proj2[2 * ruleNumber + 1] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);

			try {
				inl2 = new NestedLoopsJoins(Ntypes2, 2 * ruleNumber, Nsizes2, Ntypes, 2, Nsizes, 10, inl, "nodes.in",
						outFilter, null, proj2, 2 * ruleNumber + 2);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			inl = inl2;
			ruleNumber++;
		}

		Tuple t = new Tuple();
		// AttrType[] jtype = new AttrType[4];
		//
		// jtype[0] = new AttrType(AttrType.attrString);
		// jtype[1] = new AttrType(AttrType.attrInterval);
		// jtype[2] = new AttrType(AttrType.attrString);
		// jtype[3] = new AttrType(AttrType.attrInterval);
		//
		AttrType[] jtype = new AttrType[2 * ruleNumber + 2];

		for (int i = 0; i < 2 * ruleNumber; i++) {
			if (i % 2 == 0) {
				jtype[i] = new AttrType(AttrType.attrString);
			} else {
				jtype[i] = new AttrType(AttrType.attrInterval);
			}
		}

//		Tuple t = new Tuple();
//		AttrType[] jtype = new AttrType[4];
//
//		jtype[0] = new AttrType(AttrType.attrString);
//		jtype[1] = new AttrType(AttrType.attrInterval);
//		jtype[2] = new AttrType(AttrType.attrString);
//		jtype[3] = new AttrType(AttrType.attrInterval);

		try {
			while ((t = inl2.get_next()) != null) {
				t.print(jtype);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

	}
*/

	public void Query6()
	{
		System.out.print("**********************Query6 strating *********************\n");
		boolean status = OK;
		// Sailors, Boats, Reserves Queries.
		System.out.print("Query: Find the names of sailors with a rating greater than 7\n"
				+ "  who have reserved a red boat, and print them out in sorted order.\n\n" + "  SELECT   S.sname\n"
				+ "  FROM     Sailors S, Boats B, Reserves R\n"
				+ "  WHERE    S.sid = R.sid AND S.rating > 7 AND R.bid = B.bid \n" + "           AND B.color = 'red'\n"
				+ "  ORDER BY S.name\n\n"

				+ "Plan used:\n"
				+ " SorQuery6t(Pi(sname) (Sigma(B.color='red')  |><|  Pi(sname, bid) (Sigma(S.rating > 7)  |><|  R)))\n\n"

				+ "(Tests FileScan, Multiple Selection, Projection,sort and nested-loop join.)\n\n");

		CondExpr[] outFilter = new CondExpr[3];
		outFilter[0] = new CondExpr();
		outFilter[1] = new CondExpr();
		outFilter[2] = new CondExpr();
		CondExpr[] outFilter2 = new CondExpr[3];
		outFilter2[0] = new CondExpr();
		outFilter2[1] = new CondExpr();
		outFilter2[2] = new CondExpr();

		Query6_CondExpr(outFilter, outFilter2);
		Tuple t = new Tuple();
		t = null;

		AttrType[] Stypes = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrReal) };

		short[] Ssizes = new short[1];
		Ssizes[0] = 30;
		AttrType[] Rtypes = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger),
				new AttrType(AttrType.attrString), };

		short[] Rsizes = new short[1];
		Rsizes[0] = 15;
		AttrType[] Btypes = { new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrString),
				new AttrType(AttrType.attrString), };

		short[] Bsizes = new short[2];
		Bsizes[0] = 30;
		Bsizes[1] = 20;

		AttrType[] Jtypes = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger), };

		short[] Jsizes = new short[1];
		Jsizes[0] = 30;
		AttrType[] JJtype = { new AttrType(AttrType.attrString), };

		short[] JJsize = new short[1];
		JJsize[0] = 30;

		FldSpec[] proj1 = { new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 2) }; // S.sname,
		// R.bid

		FldSpec[] proj2 = { new FldSpec(new RelSpec(RelSpec.outer), 1) };

		FldSpec[] Sprojection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 3),
				new FldSpec(new RelSpec(RelSpec.outer), 4) };

		FileScan am = null;
		try {
			am = new FileScan("sailors.in", Stypes, Ssizes, (short) 4, (short) 4, Sprojection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}

		if (status != OK) {
			// bail out

			System.err.println("*** Error setting up scan for sailors");
			Runtime.getRuntime().exit(1);
		}

		NestedLoopsJoins inl = null;
		try {
			inl = new NestedLoopsJoins(Stypes, 4, Ssizes, Rtypes, 3, Rsizes, 10, am, "reserves.in", outFilter, null,
					proj1, 2);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.print("After nested loop join S.sid|><|R.sid.\n");

		NestedLoopsJoins nlj = null;
		try {
			nlj = new NestedLoopsJoins(Jtypes, 2, Jsizes, Btypes, 3, Bsizes, 10, inl, "boats.in", outFilter2, null,
					proj2, 1);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		System.out.print("After nested loop join R.bid|><|B.bid AND B.color=red.\n");

		TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
		Sort sort_names = null;
		try {
			sort_names = new Sort(JJtype, (short) 1, JJsize, (iterator.Iterator) nlj, 1, ascending, JJsize[0], 10);
		} catch (Exception e) {
			System.err.println("*** Error preparing for sorting");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		System.out.print("After sorting the output tuples.\n");

		QueryCheck qcheck6 = new QueryCheck(6);

		try {
			while ((t = sort_names.get_next()) != null) {
				t.print(JJtype);
				qcheck6.Check(t);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		qcheck6.report(6);

		System.out.println("\n");
		try {
			sort_names.close();
		} catch (Exception e) {
			status = FAIL;
			e.printStackTrace();
		}

		if (status != OK) {
			// bail out

			Runtime.getRuntime().exit(1);
		}

	}


	private void Disclaimer() {
		System.out.print ("\n\nAny resemblance of persons in this database to"
				+ " people living or dead\nis purely coincidental. The contents of "
				+ "this database do not reflect\nthe views of the University,"
				+ " the Computer  Sciences Department or the\n"
				+ "developers...\n\n");
	}
}

public class JoinTest
{
	public static void main(String argv[])
	{
		boolean sortstatus;
		//SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
		//JavabaseDB.openDB("/tmp/nwangdb", 5000);

		JoinsDriver jjoin = new JoinsDriver();

		sortstatus = jjoin.runTests();
		if (sortstatus != true) {
			System.out.println("Error ocurred during join tests");
		}
		else {
			System.out.println("join tests completed successfully");
		}
	}
}

