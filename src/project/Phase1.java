package project;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import btree.BTreeFile;
import btree.IntegerKey;
import btree.StringKey;
import bufmgr.BufMgr;
import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import intervalTree.IntervalKey;
import intervalTree.IntervalTreeFile;
import iterator.*;
import iterator.Iterator;
import xmlparser.XMLToIntervalTable;

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
	public static final int NUMBUF = 10000;
	public static final int TAG_LENGTH = 5;
	private boolean OK = true;
	private boolean FAIL = false;
	public Vector<NodeTable> nodes;
	private String input_file_base = "/home/renil/github/dbmsi/input/";
	private Map<String, String> tagMapping = new HashMap<>(); // contains id to tag name mapping
	public Phase1() {
/*		nodes = new Vector<NodeTable>();
		nodes.addElement(new NodeTable("A", new IntervalType(1, 14, 1)));
		nodes.addElement(new NodeTable("B", new IntervalType(2, 9, 2)));
		nodes.addElement(new NodeTable("B", new IntervalType(10, 11, 2)));
		nodes.addElement(new NodeTable("B", new IntervalType(12, 13, 2)));
		nodes.addElement(new NodeTable("C", new IntervalType(3, 6, 3)));
		nodes.addElement(new NodeTable("D", new IntervalType(7, 8, 3)));
		nodes.addElement(new NodeTable("E", new IntervalType(4, 5, 4)));
//
*/
//		nodes = XMLToIntervalTable.xmlToTreeConverter(input_file_base + "xml_sample_data.xml");
		nodes = XMLToIntervalTable.xmlToTreeConverter(input_file_base + "sample.xml");

		boolean status = OK;

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

		SystemDefs sysdef = new SystemDefs(dbpath, 10000, NUMBUF, "Clock");

		// creating the node table relation
		AttrType[] nodeTableAttrTypes = new AttrType[2];
		nodeTableAttrTypes[0] = new AttrType(AttrType.attrInterval);
		nodeTableAttrTypes[1] = new AttrType(AttrType.attrString);

		short[] nodeTableStringSizes = new short[2];
		nodeTableStringSizes[0] = 64;
		nodeTableStringSizes[1] = TAG_LENGTH;

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

		int numnodes = nodes.size();
		for (int i = 0; i < numnodes; i++) {
			System.out.println(i +" "+ nodes.elementAt(i).toString());
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
		
		//-----------------------------------------creating b tree index----------------------------------
		// create an scan on the heapfile
	    Scan scan = null;
	    
	    try {
	      scan = new Scan(f);
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	      Runtime.getRuntime().exit(1);
	    }

	    // create the index file on the integer field
	    BTreeFile btf = null;
	    try {
	      btf = new BTreeFile("BTIndex", AttrType.attrString, 5, 1/*delete*/); 
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	      Runtime.getRuntime().exit(1);
	    }

	    System.out.println("BTreeIndex created successfully.\n"); 
	    
	    rid = new RID();
	    String key = "";
	    Tuple temp = null;
	    
	    try {
	      temp = scan.getNext(rid);
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	    }
	    while ( temp != null) {
	      t.tupleCopy(temp);
	      
	      try {
		key = t.getStrFld(2);
		if(key.length() > 3) key = key.substring(0, 3);
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	      
	      try {
		btf.insert(new StringKey(key), rid); 
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }

	      try {
		temp = scan.getNext(rid);
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	    }
	    
	    // close the file scan
	    scan.closescan();
	    
	    System.out.println("---------------------------------BTreeIndex file on tag created successfully---------------------------------------.\n");
	   
	    
	    // create an scan on the heapfile
	    scan = null;
	    
	    try {
	      scan = new Scan(f);
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	      Runtime.getRuntime().exit(1);
	    }

	    // create the index file on the integer field
	    IntervalTreeFile btfInterval = null;
	    try {
	    	btfInterval = new IntervalTreeFile("IntervalIndex", 1/*delete*/); 
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	      Runtime.getRuntime().exit(1);
	    }

	    System.out.println("BTreeIndex created successfully.\n"); 
	    
	    rid = new RID();
	    IntervalType intervalType = null;
	    temp = null;
	    
	    try {
	      temp = scan.getNext(rid);
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	    }
	    while ( temp != null) {
	      t.tupleCopy(temp);
	      
	      try {
	    	  
	    	  String s = t.getStrFld(2);
	    	  intervalType = t.getIntervalField(1);
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	      
	      try {
	    	  btfInterval.insert(new IntervalKey(intervalType), rid); 
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }

	      try {
		temp = scan.getNext(rid);
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	    }
	    
	    // close the file scan
	    scan.closescan();
	    
	    System.out.println("BTreeIndex file created successfully.\n"); 
	    
	    FldSpec[] projlist = new FldSpec[2];
	    RelSpec rel = new RelSpec(RelSpec.outer); 
	    projlist[0] = new FldSpec(rel, 1);
	    projlist[1] = new FldSpec(rel, 2);
	    
	    // conditions
	    CondExpr[] expr = new CondExpr[3]; 
	    expr[0] = new CondExpr();
	    expr[0].op = new AttrOperator(AttrOperator.aopGE);
	    expr[0].type1 = new AttrType(AttrType.attrSymbol);
	    expr[0].type2 = new AttrType(AttrType.attrInterval);
	    expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    expr[0].operand2.interval = new IntervalType(207, 888, 3);
	    expr[0].next = null;
//	    expr[1] = null;
	    
	    expr[1] = new CondExpr();
	    expr[1].op = new AttrOperator(AttrOperator.aopLE);
	    expr[1].type1 = new AttrType(AttrType.attrSymbol);
	    expr[1].type2 = new AttrType(AttrType.attrInterval);
	    expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
	    expr[1].operand2.interval = new IntervalType(210, 908, 3);
	    expr[1].next = null;
	    expr[2] = null;

	    // start index scan
	    IndexScan iscan = null;
	    try {
	      iscan = new IndexScan(new IndexType(IndexType.interval_Index), "nodes.in", "IntervalIndex", nodeTableAttrTypes, nodeTableStringSizes, 2, 2, projlist, expr, 1, false);
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	    }
	    

	    t = null;
	    IntervalType iout = null;
	    int ival = 100, count =0; // low key
	    
	    try {
	      t = iscan.get_nextInterval();
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace(); 
	    }

	    while (t != null) {
	      try {
		iout = t.getIntervalField(1);
		System.out.println("count " + (++count));
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	      
//	      if (iout < ival) {
//		System.err.println("count = "  + " iout = " + iout + " ival = " + ival);
//		
//		System.err.println("Test3 -- OOPS! index scan not in sorted order");
//		status = FAIL;
//		break; 
//	      }
//	      else if (iout > 900) {
//		System.err.println("Test 3 -- OOPS! index scan passed high key");
//		status = FAIL;
//		break;
//	      }
	      
//	      ival = iout;
	      System.out.println("result "+ iout.toString());
	      
	      try {
		t = iscan.get_nextInterval();
	      }
	      catch (Exception e) {
		status = FAIL;
		e.printStackTrace();
	      }
	    }
	    if (status) {
	      System.err.println("Test3 -- Index scan on int key OK\n");
	    }

	    // clean up
	    try {
	      iscan.close();
	    }
	    catch (Exception e) {
	      status = FAIL;
	      e.printStackTrace();
	    }
	    
		
	}

	private void Project2_CondExpr(CondExpr[] expr, Rule rule) {

		String outerNode = rule.outerTag;
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
		expr[2].operand2.string = rule.innerTag;

		expr[3] = null;
	}


	private void Project4_CondExpr(CondExpr[] OutFilter, Rule rule, int offset) {

		String outerNode = rule.outerTag;

		OutFilter[0].next = null;
		OutFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		OutFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		OutFilter[0].type2 = new AttrType(AttrType.attrSymbol);
		OutFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset);
		OutFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
		OutFilter[0].flag =0;

	}


	private CondExpr[] createFilterForQueryHeap(List<Rule> rules) {
		Set<String> uniqueTags = new HashSet<>();
		for (Rule rule : rules) {
			String tag1 = tagMapping.get(rule.outerTag);
			String tag2 = tagMapping.get(rule.innerTag);
			uniqueTags.add(tag1);
			uniqueTags.add(tag2);
		}

		CondExpr[] innerRelFilterConditions = new CondExpr[2];
		CondExpr prev = null;
		CondExpr curr = null;
		CondExpr head =null;
		for (String tag : uniqueTags) {
			curr = new CondExpr();
			curr.next = null;
			curr.op = new AttrOperator(AttrOperator.aopEQ);
			curr.type1 = new AttrType(AttrType.attrSymbol);
			curr.type2 = new AttrType(AttrType.attrString);
			curr.operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
			curr.operand2.string = tag;

			//Inner table comparison.
			if (prev != null) {
				prev.next = curr;
				prev = curr;
			} else {
				prev = curr;
				head = curr;
			}

		}

		innerRelFilterConditions[0] = head;
		return innerRelFilterConditions;

	}

	private void createQueryHeapFile(String file, List<Rule> rules) {
		Iterator fileScanner = null;
		System.out.println("Creating temp heap file for query");
		AttrType[] baseTableAttrTypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = TAG_LENGTH;

		AttrType[] nodeTableAttrTypes = new AttrType[2];
		nodeTableAttrTypes[0] = new AttrType(AttrType.attrInterval);
		nodeTableAttrTypes[1] = new AttrType(AttrType.attrString);

		short[] nodeTableStringSizes = new short[1];
		nodeTableStringSizes[0] = TAG_LENGTH;

		CondExpr[] innerRelFilterConditions = createFilterForQueryHeap(rules);

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		try {

			fileScanner = new FileScan(file, baseTableAttrTypes, baseTableStringLengths, (short) 2, (short) 2,
					initialProjection, innerRelFilterConditions);
		} catch (Exception e) {
			System.err.println("" + e);
			e.printStackTrace();
		}

		Heapfile temp = null;

		try {
			temp = new Heapfile("temp.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			e.printStackTrace();
		}

		try {
			Tuple t =fileScanner.get_next();

			while (t != null) {
				Tuple tuple = new Tuple(t.getLength());
				tuple.setHdr((short) 2, nodeTableAttrTypes, nodeTableStringSizes);
				tuple.setIntervalFld(1, t.getIntervalField(1));
				tuple.setStrFld(2, t.getStrFld(2));
				temp.insertRecord(tuple.returnTupleByteArray());
				t = fileScanner.get_next();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void setConditions(CondExpr[] outFilter, CondExpr[] rightFilter, Rule rule, int offset, boolean isFirstRule) {

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
			outFilter[0].flag = CondExpr.FLAG_PC_CHECK;
		} else {
			outFilter[0].flag = CondExpr.FLAG_AD_CHECK;
		}

		//Outer table comparison. For eg: If rule is A B PC, this condition will return
		//results for outer table where tag name equals to A.
		outFilter[1].next = null;
		outFilter[1].op = new AttrOperator(AttrOperator.aopEQ);
		outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
		outFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), outerTagNameColNo);
		outFilter[1].type2 = new AttrType(AttrType.attrString);
		outFilter[1].operand2.string = tagMapping.get(rule.outerTag);

		outFilter[2] = null;

		//Inner table comparison.
		rightFilter[0].next = null;
		rightFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
		rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
		rightFilter[0].type2 = new AttrType(AttrType.attrString);
		rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		rightFilter[0].operand2.string = tagMapping.get(rule.innerTag);

		rightFilter[1] = null;
	}

	private void populateNodeOffsetMap(Map<String, Integer> offsetMap, String nodeName, int nodeNumber) {
		offsetMap.put(nodeName, 2*nodeNumber);
	}

	public void compute(List<Rule> rules) {
//		Rule rule1 = new Rule("A", "B", Rule.RULE_TYPE_PARENT_CHILD);
//		Rule rule2 = new Rule("A", "E", Rule.RULE_TYPE_ANCESTRAL_DESCENDENT);
//		Rule rule3 = new Rule("B", "C", Rule.RULE_TYPE_PARENT_CHILD);
//
//		//Rule rule3 = new Rule("B", "D", Rule.RULE_TYPE_ANCESTRAL_DESCENDENT);
//		ArrayList<Rule> rules = new ArrayList<>();
//		rules.add(rule1);
//		rules.add(rule2);
//		rules.add(rule3);

		// Map containing the corresponding column number for the
		// given tag Id in the joined table.
		HashMap<String, Integer> tagOffsetMap = new HashMap<>();

		int nodeNumber = 1;
		boolean status = OK;

		Iterator fileScanner = null;

		AttrType[] baseTableAttrTypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = TAG_LENGTH;

		Rule firstRule = rules.get(0);

		CondExpr[] innerRelFilterConditions = new CondExpr[2];
		innerRelFilterConditions[0] = new CondExpr();
		innerRelFilterConditions[1] = new CondExpr();

		//Inner table comparison.
		innerRelFilterConditions[0].next = null;
		innerRelFilterConditions[0].op = new AttrOperator(AttrOperator.aopEQ);
		innerRelFilterConditions[0].type1 = new AttrType(AttrType.attrSymbol);
		innerRelFilterConditions[0].type2 = new AttrType(AttrType.attrString);
		innerRelFilterConditions[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		innerRelFilterConditions[0].operand2.string = tagMapping.get(firstRule.outerTag);

		innerRelFilterConditions[1] = null;

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		try {

			fileScanner = new FileScan("temp.in", baseTableAttrTypes, baseTableStringLengths, (short) 2, (short) 2,
					initialProjection, innerRelFilterConditions);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}


		populateNodeOffsetMap(tagOffsetMap, firstRule.outerTag, nodeNumber);
		nodeNumber++;
		populateNodeOffsetMap(tagOffsetMap, firstRule.innerTag, nodeNumber);
		nodeNumber++;

		CondExpr[] filterConditions = new CondExpr[4];
		filterConditions[0] = new CondExpr();
		filterConditions[1] = new CondExpr();
		filterConditions[2] = new CondExpr();
		filterConditions[3] = new CondExpr();

		innerRelFilterConditions = new CondExpr[2];
		innerRelFilterConditions[0] = new CondExpr();
		innerRelFilterConditions[1] = new CondExpr();

		setConditions(filterConditions, innerRelFilterConditions, firstRule, 1, true);
		FldSpec[] currProjection = { new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.innerRel), 2),
				new FldSpec(new RelSpec(RelSpec.innerRel), 1) };

		NestedLoopsJoins prevIterator = null;
		NestedLoopsJoins currIterator = null;
		try {
			prevIterator = new NestedLoopsJoins(baseTableAttrTypes, 2, baseTableStringLengths, baseTableAttrTypes, 2,
					baseTableStringLengths, 10, fileScanner, "temp.in", filterConditions, innerRelFilterConditions, currProjection, 4);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		//Needs to iterate only from the second rule.
		//rules.remove(0);
		int ruleNumber = 2;
		for (int index = 1; index < rules.size(); index++) {
			Rule currRule = rules.get(index);
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

			innerRelFilterConditions = new CondExpr[2];
			innerRelFilterConditions[0] = new CondExpr();
			innerRelFilterConditions[1] = new CondExpr();
			setConditions(filterConditions, innerRelFilterConditions, currRule, tagOffsetMap.get(currRule.outerTag), false);

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
				joinedTableStringLengths[i] = TAG_LENGTH;
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
						baseTableStringLengths, 10, prevIterator, "temp.in", filterConditions, innerRelFilterConditions, currProjection,
						2 * ruleNumber + 2);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			prevIterator = currIterator;
			ruleNumber++;

		}

		if(currIterator == null) {
			currIterator = prevIterator;
		}
		Tuple finalTuple = new Tuple();
		AttrType[] finalTupleAttrTypes = new AttrType[2 * ruleNumber];

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

	public void computeSM(List<Rule> rules, TupleOrder order) {
		Map<String, Integer> nodeOffsetMap = new HashMap<>();
		//rules.add(rule3);
		int nodeNumber = 1;
		boolean status = OK;

		Iterator am = null;
		Iterator am1 = null;
		AttrType[] Ntypes = {new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString)};
		short[] Nsizes = new short[1];
		Nsizes[0] = TAG_LENGTH;

		FldSpec[] Nprojection = {new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2)};


		FldSpec[] proj = {new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.innerRel), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 1)};
		List<NestedLoopsJoins> listNLJ = new LinkedList<>();
		for (Rule rule : rules) {
			try {
				CondExpr[] innerRelFilterConditions = new CondExpr[2];
				innerRelFilterConditions[0] = new CondExpr();
				innerRelFilterConditions[1] = new CondExpr();

				//Inner table comparison.
				innerRelFilterConditions[0].next = null;
				innerRelFilterConditions[0].op = new AttrOperator(AttrOperator.aopEQ);
				innerRelFilterConditions[0].type1 = new AttrType(AttrType.attrSymbol);
				innerRelFilterConditions[0].type2 = new AttrType(AttrType.attrString);
				innerRelFilterConditions[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
				innerRelFilterConditions[0].operand2.string = tagMapping.get(rule.outerTag);

				innerRelFilterConditions[1] = null;
				am = new FileScan("temp.in", Ntypes, Nsizes, (short) 2, (short) 2, Nprojection, innerRelFilterConditions);
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

				CondExpr[] innerRelFilterConditions = new CondExpr[2];
				innerRelFilterConditions[0] = new CondExpr();
				innerRelFilterConditions[1] = new CondExpr();

				setConditions(outFilter,innerRelFilterConditions, rule, 1, true);
				inl = new NestedLoopsJoins(Ntypes, 2, Nsizes, Ntypes, 2, Nsizes, 10, am, "temp.in", outFilter, innerRelFilterConditions, proj,
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
		populateNodeOffsetMap(nodeOffsetMap, prevRule.outerTag, nodeNumber);
		nodeNumber++;
		populateNodeOffsetMap(nodeOffsetMap, prevRule.innerTag, nodeNumber);
		nodeNumber++;
		Iterator prevSM = listNLJ.get(0);
		int index = 2;
		String prevSortedTag = "";
		for (int x = 1; x < listNLJ.size(); ++x) {
			if (!nodeOffsetMap.containsKey(rules.get(x).outerTag)) {
				//Technically, this should never happen.
				populateNodeOffsetMap(nodeOffsetMap, rules.get(x).outerTag, nodeNumber);
				nodeNumber++;
			}

			if (!nodeOffsetMap.containsKey(rules.get(x).innerTag)) {
				populateNodeOffsetMap(nodeOffsetMap, rules.get(x).innerTag, nodeNumber);
				nodeNumber++;
			}
			try {
				CondExpr[] outFilter = new CondExpr[2];
				outFilter[0] = new CondExpr();
				outFilter[1] = null;

				Project4_CondExpr(outFilter, rules.get(x), nodeOffsetMap.get(rules.get(x).outerTag));
				AttrType[] NtypesFix = {new AttrType(AttrType.attrString),
						new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInterval)};
				short[] NsizesFix = new short[2];
				NsizesFix[0] = TAG_LENGTH;
				NsizesFix[1] = TAG_LENGTH;
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
					Nsizes2[i] = TAG_LENGTH;
				}
				FldSpec[] proj2 = new FldSpec[2 * index + 2];
				for (int i = 0; i < 2 * index; i++) {
					proj2[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
				}
				proj2[2 * index] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
				proj2[2 * index + 1] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);
				SortMerge sm = null;
				if(!prevSortedTag.equalsIgnoreCase(rules.get(x).outerTag)) {
					sm = new SortMerge(Ntypes2, index * 2, Nsizes2,
							NtypesFix, 4, NsizesFix,
							nodeOffsetMap.get(rules.get(x).outerTag), 4,
							2, 4,
							100,
							prevSM, listNLJ.get(x),
							false, false, order,
							outFilter, proj2, 2 * index + 2);
				} else {
					sm = new SortMerge(Ntypes2, index * 2, Nsizes2,
							NtypesFix, 4, NsizesFix,
							nodeOffsetMap.get(rules.get(x).outerTag), 4,
							2, 4,
							100,
							prevSM, listNLJ.get(x),
							true, false, order,
							outFilter, proj2, 2 * index + 2);
				}
				if (!prevRule.innerTag.equals(rules.get(x).outerTag)) {
					sm.setCheckFlag(true);
				} else
					sm.setCheckFlag(false);
				index++;
				prevSM = sm;
				prevRule = rules.get(x);
			} catch (Exception e) {
				System.err.println("*** join error in SortMerge constructor ***");
				status = FAIL;
				System.err.println("" + e);
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
			int count = 1;
			while ((t = prevSM.get_next()) != null) {
				System.out.println("Result " + count++ + ":");
				t.print(jtype);
			}
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

	}


	private String[] readFile(String filename) {
		StringBuffer stringBuffer = new StringBuffer();
		try {
			File file = new File(filename);
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String line;
			while ((line = bufferedReader.readLine()) != null) {
				stringBuffer.append(line);
				stringBuffer.append("\n");
			}
			fileReader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stringBuffer.toString().split("\n");
	}

	private List<Rule> getRuleList(String[] file_contents) {
		int n = Integer.parseInt(file_contents[0]);
		int index = 0;
		int[] rankIndex = new int[n];
		tagMapping.clear();
		Map<Integer, List<Rule>> ruleMap = new HashMap<>();
		for (String line : file_contents) {
			if (index > 0 && index <= n) {
				ruleMap.put(index, new ArrayList<>());
                tagMapping.put(Integer.toString(index), XMLToIntervalTable.trimCharTags(line));
			}

			if (index > n) {
				String[] rule_components = line.split(" ");
				int relation = rule_components[2].equals("PC") ? Rule.RULE_TYPE_PARENT_CHILD : Rule.RULE_TYPE_ANCESTRAL_DESCENDENT;
				rankIndex[Integer.parseInt(rule_components[1]) - 1]++;
				ruleMap.get(Integer.parseInt(rule_components[0])).add(new Rule(rule_components[0], rule_components[1], relation));
			}
			index++;
		}
		Integer root = getRoot(rankIndex);
		return getRulesInOrder(root, ruleMap);
	}

	private void input() {
		String choice = "Y";
		System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
		while (!choice.equals("N") && !choice.equals("n")) {
			// SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");
			BufMgr.page_access_counter = 0;

			System.out.println("Enter input filename for query");
			Scanner scanner = new Scanner(System.in);
			String file = scanner.next();
			System.out.println("QUERY PLAN---1");
			String[] file_contents = readFile(input_file_base + file);
			List<Rule> rules = getRuleList(file_contents);
			createQueryHeapFile("nodes.in", rules);
			compute(rules);
			System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
			//sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");
			BufMgr.page_access_counter = 0;
			System.out.println("QUERY PLAN---2");
			computeSM(rules, new TupleOrder(TupleOrder.Ascending));
			System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
			//sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");
			BufMgr.page_access_counter = 0;
			System.out.println("QUERY PLAN---3");
			computeSM(rules, new TupleOrder(TupleOrder.Descending));
//			System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
			System.out.println("Press N to stop");
			choice = scanner.next();
			System.out.print(choice);
		}
	}

	private void printRules(List<Rule> rules) {
		System.out.print("Printing rules " + rules.size() );
		for (Rule rule : rules)
			System.out.println(rule.outerTag + " " + rule.innerTag + rule.getRelationship());
	}

	List<Rule> getRulesInOrder(Integer root, Map<Integer, List<Rule>> ruleMap) {
		List<Rule>  orderedList = new ArrayList<>();
		Queue<Integer> queue = new ArrayDeque<>();
		queue.add(root);
		while (!queue.isEmpty()) {
			Integer node = queue.remove();
			List<Rule> childRules = ruleMap.get(node);
			if (childRules != null) {
				for (Rule rule : childRules) {
					queue.add(Integer.parseInt(rule.innerTag));
				}
				orderedList.addAll(childRules);
			}
		}
		return orderedList;
	}



	private Integer getRoot(int[] rankIndex) {
		for (int i = 0; i < rankIndex.length; i++)
			if (rankIndex[i] == 0)
				return i + 1;
		return -1;
	}

	public static void main(String[] args) {
		
		
		
		Phase1 phase1 = new Phase1();
//		phase1.input();
		
		//phase1.compute();
		//phase1.computeSM();
	}
}
