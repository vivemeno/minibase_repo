package project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Vector;

import global.AttrOperator;
import global.AttrType;
import global.IntervalType;
import global.RID;
import global.SystemDefs;
import heap.Heapfile;
import heap.Tuple;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.NestedLoopsJoins;
import iterator.RelSpec;

class NodeTable {
	public String nodename;
	public IntervalType interval;
	
	public NodeTable (String _nodeName, IntervalType _interval) {
		nodename = _nodeName;
		interval = _interval;
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

public class Phase1 {
	public static final int NUMBUF = 50;
	private boolean OK = true;
	private boolean FAIL = false;
	public Vector nodes;

	public Phase1() {
		nodes = new Vector();
		nodes.addElement(new NodeTable("A", new IntervalType(1, 6, 1)));
		nodes.addElement(new NodeTable("B", new IntervalType(2, 3, 2)));
		// nodes.addElement(new NodeTable("C", new IntervalType(3, 4, 3)));
		// nodes.addElement(new NodeTable("B", new IntervalType(5, 6, 2)));
		nodes.addElement(new NodeTable("E", new IntervalType(4, 5, 2)));
		// nodes.addElement(new NodeTable("F", new IntervalType(9, 12, 3)));
		// nodes.addElement(new NodeTable("G", new IntervalType(10, 11, 4)));
		
		boolean status = OK;
		int numnodes = 3;
		// int numnodes_attrs = 2;

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

		SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");

		// creating the node table relation
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

		// inserting the tuple into file "nodes"
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
			System.err.println("*** Error creating relation for nodes");
			Runtime.getRuntime().exit(1);
		}
	}

	private void Project2_CondExpr(CondExpr[] expr, CondExpr[] expr2, Rule rule, int nodeIndex) {

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

		expr2[0].next = null;
		expr2[0].op = new AttrOperator(AttrOperator.aopEQ);
		expr2[0].type1 = new AttrType(AttrType.attrSymbol);
		expr2[0].type2 = new AttrType(AttrType.attrSymbol);
		expr2[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2 * nodeIndex - 1);
		expr2[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

		expr2[1] = null;
	}

	private void Project3_CondExpr(CondExpr[] RightFilter, CondExpr[] OutFilter, Rule rule, int nodeIndex) {

		String outerNode = rule.outerRule;
		RightFilter[0].next = null;
		RightFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		RightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		RightFilter[0].type2 = new AttrType(AttrType.attrString);
		RightFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		RightFilter[0].operand2.string = rule.innerRule;

		RightFilter[1] = null;

		OutFilter[0].next = null;
		OutFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		OutFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		OutFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		OutFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), (2 * nodeIndex) - 1);
		OutFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
		if (rule.ruleType == Rule.RULE_TYPE_PARENT_CHILD) {
			OutFilter[0].flag = 2;
		} else {
			OutFilter[0].flag = 1;
		}

		OutFilter[1].next = null;
		OutFilter[1].op = new AttrOperator(AttrOperator.aopEQ);
		OutFilter[1].type1 = new AttrType(AttrType.attrSymbol);
		OutFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		OutFilter[1].type2 = new AttrType(AttrType.attrString);
		OutFilter[1].operand2.string = outerNode;
		OutFilter[2] = null;
	}

	public void compute() {
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
				proj2[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
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
	
	public static void main(String[] args) {
		Phase1 phase1 = new Phase1();
		phase1.compute();
	}
}
