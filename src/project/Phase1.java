package project;

import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
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
	public String outerTag;
	public String innerTag;
	public int ruleType;
	
	public static int RULE_TYPE_PARENT_CHILD = 0;
	public static int RULE_TYPE_ANCESTRAL_DESCENDENT = 1;
	
	public Rule(String _outerTag, String _innerTag, int _ruleType) {
		this.outerTag = _outerTag;
		this.innerTag = _innerTag;
		this.ruleType = _ruleType;
	}
	
	public String getRelationship() {
		if(ruleType == RULE_TYPE_PARENT_CHILD) {
			return "PC";
		} 
		return "AD";
	}
	
	@Override
	public String toString() {
		return "Rule :["+ outerTag + " " + innerTag + " " + getRelationship() + "]";
	}
}

public class Phase1 {
	public static final int NUMBUF = 50;
	public static final int TAG_LENGTH = 1;
	private boolean OK = true;
	private boolean FAIL = false;
	public Vector<NodeTable> nodes;

	public Phase1() {
		nodes = new Vector<NodeTable>();
		nodes.addElement(new NodeTable("A", new IntervalType(1, 14, 1)));
		nodes.addElement(new NodeTable("B", new IntervalType(2, 5, 2)));
		nodes.addElement(new NodeTable("B", new IntervalType(6, 11, 2)));
		nodes.addElement(new NodeTable("B", new IntervalType(12, 13, 2)));
		nodes.addElement(new NodeTable("E", new IntervalType(3, 4, 3)));
		nodes.addElement(new NodeTable("E", new IntervalType(7, 8, 3)));
		nodes.addElement(new NodeTable("D", new IntervalType(9, 10, 3)));
		
		boolean status = OK;
		int numnodes = 7;

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
		AttrType[] nodeTableAttrTypes = new AttrType[2];
		nodeTableAttrTypes[0] = new AttrType(AttrType.attrInterval);
		nodeTableAttrTypes[1] = new AttrType(AttrType.attrString);

		short[] nodeTableStringSizes = new short[1];
		nodeTableStringSizes[0] = TAG_LENGTH; 

		Tuple t = new Tuple();
		try {
			t.setHdr((short) 2, nodeTableAttrTypes, nodeTableStringSizes);
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
			t.setHdr((short) 2, nodeTableAttrTypes, nodeTableStringSizes);
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
		outFilter[1].operand2.string = rule.outerTag;
		
		//Inner table comparison.
		outFilter[2].next = null;
		outFilter[2].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[2].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[2].type2 = new AttrType(AttrType.attrString);
		outFilter[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		outFilter[2].operand2.string = rule.innerTag;
		
		outFilter[3] = null;
	}

	private void populateNodeOffsetMap(Map<String, Integer> offsetMap, String nodeName, int nodeNumber) {
		offsetMap.put(nodeName, 2*nodeNumber);
	}
	
	public void compute() {
		Rule rule1 = new Rule("A", "B", Rule.RULE_TYPE_PARENT_CHILD);
		Rule rule2 = new Rule("B", "E", Rule.RULE_TYPE_PARENT_CHILD);
		Rule rule3 = new Rule("B", "D", Rule.RULE_TYPE_ANCESTRAL_DESCENDENT);
		ArrayList<Rule> rules = new ArrayList<>();
		rules.add(rule1);
		rules.add(rule2);
		rules.add(rule3);

		// Map containing the corresponding column number for the
		// given tag Id in the joined table.
		HashMap<String, Integer> tagOffsetMap = new HashMap<>();

		int nodeNumber = 1;
		boolean status = OK;

		Iterator fileScanner = null;

		AttrType[] baseTableAttrTypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = TAG_LENGTH;

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		try {
			fileScanner = new FileScan("nodes.in", baseTableAttrTypes, baseTableStringLengths, (short) 2, (short) 2,
					initialProjection, null);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}

		Rule firstRule = rules.get(0);
		populateNodeOffsetMap(tagOffsetMap, firstRule.outerTag, nodeNumber);
		nodeNumber++;
		populateNodeOffsetMap(tagOffsetMap, firstRule.innerTag, nodeNumber);
		nodeNumber++;

		CondExpr[] filterConditions = new CondExpr[4];
		filterConditions[0] = new CondExpr();
		filterConditions[1] = new CondExpr();
		filterConditions[2] = new CondExpr();
		filterConditions[3] = new CondExpr();

		setConditions(filterConditions, firstRule, 1, true);
		FldSpec[] currProjection = { new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.innerRel), 2),
				new FldSpec(new RelSpec(RelSpec.innerRel), 1) };
		
		NestedLoopsJoins prevIterator = null;
		NestedLoopsJoins currIterator = null;
		try {
			prevIterator = new NestedLoopsJoins(baseTableAttrTypes, 2, baseTableStringLengths, baseTableAttrTypes, 2,
					baseTableStringLengths, 10, fileScanner, "nodes.in", filterConditions, null, currProjection, 4);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		System.out.println(firstRule);
		//Needs to iterate only from the second rule.
		rules.remove(0);
		int ruleNumber = 2;
		for (Rule currRule : rules) {
			if (!tagOffsetMap.containsKey(currRule.outerTag)) {
				// Technically, this should never happen, since the input tree is connected.
				populateNodeOffsetMap(tagOffsetMap, currRule.outerTag, nodeNumber);
				nodeNumber++;
			}

			if (!tagOffsetMap.containsKey(currRule.innerTag)) {
				populateNodeOffsetMap(tagOffsetMap, currRule.innerTag, nodeNumber);
				nodeNumber++;
			}

			filterConditions = new CondExpr[4];
			filterConditions[0] = new CondExpr();
			filterConditions[1] = new CondExpr();
			filterConditions[2] = new CondExpr();
			filterConditions[3] = new CondExpr();
			setConditions(filterConditions, currRule, tagOffsetMap.get(currRule.outerTag), false);
			
			//After each rule the 2 more columns will be added.
			AttrType[] joinedTableAttrTypes = new AttrType[2 * ruleNumber];
			for (int i = 0; i < 2 * ruleNumber; i++) {
				if (i % 2 == 0) {
					joinedTableAttrTypes[i] = new AttrType(AttrType.attrString);
				} else {
					joinedTableAttrTypes[i] = new AttrType(AttrType.attrInterval);
				}
			}
			short[] joinedTableStringLengths = new short[ruleNumber];
			for (int i = 0; i < ruleNumber; i++) {
				joinedTableStringLengths[i] = 1;
			}
			
			//Projection size will also increase by 2 in every rule. Also, an additional 2 more columns will
			//be added after the join with the inner table. This additional 2 columns will be of the tag id
			//which has not occurred before in any of the rules. We can be assured that the outer tag of a rule 
			//will always be new since we have ordered the rules in a level wise fashion and also because of 
			//the assumption that there are no rules with a circular relationship.
			currProjection = new FldSpec[2 * ruleNumber + 2];
			for (int i = 0; i < 2 * ruleNumber; i++) {
				currProjection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}
			currProjection[2 * ruleNumber] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
			currProjection[2 * ruleNumber + 1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

			try {
				currIterator = new NestedLoopsJoins(joinedTableAttrTypes, 2 * ruleNumber, joinedTableStringLengths, baseTableAttrTypes, 2,
						baseTableStringLengths, 10, prevIterator, "nodes.in", filterConditions, null, currProjection,
						2 * ruleNumber + 2);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			prevIterator = currIterator;
			ruleNumber++;
			
		System.out.println(currRule);
		}

		Tuple finalTuple = new Tuple();
		AttrType[] finalTupleAttrTypes = new AttrType[2 * ruleNumber + 2];

		for (int i = 0; i < 2 * ruleNumber; i++) {
			if (i % 2 == 0) {
				finalTupleAttrTypes[i] = new AttrType(AttrType.attrString);
			} else {
				finalTupleAttrTypes[i] = new AttrType(AttrType.attrInterval);
			}
		}

		try {
			int count = 1;
			while ((finalTuple = currIterator.get_next()) != null) {
				System.out.println("Result " + count++ + ":");
				finalTuple.print(finalTupleAttrTypes);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		if (status != OK) {
			System.out.println(" Error Occured !!");
		}

	}
	
	public static void main(String[] args) {
		Phase1 phase1 = new Phase1();
		phase1.compute();
	}
}
