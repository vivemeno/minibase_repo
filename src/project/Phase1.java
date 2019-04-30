package project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import java.util.*;

import bufmgr.BufMgr;
import global.*;
import heap.FileAlreadyDeletedException;
import heap.HFBufMgrException;
import heap.HFDiskMgrException;
import heap.HFException;
import heap.Heapfile;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Scan;
import heap.Tuple;
import index.IndexException;
import index.IndexScan;
import intervalTree.IntervalKey;
import intervalTree.IntervalTreeFile;
import iterator.*;
import iterator.Iterator;
import xmlparser.XMLToIntervalTable;


class Statistics {
    public int totalCount;
    public double intervalRange;
    public Statistics(int totalCount, double intervalRange) {
        this.totalCount = totalCount;
        this.intervalRange = intervalRange;
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
    public static final int NUMBUF = 5000;
    public static final int TAG_LENGTH = 5;
    private boolean OK = true;
    private boolean FAIL = false;
    public Vector<NodeTable> nodes;

     private String input_file_base = "/home/akhil/MS/DBMS/";

     //private String input_file_base = "/home/vivemeno/DBMSI/input/";

// private String input_file_base = "/home/akhil/MS/DBMS/";
//    private String input_file_base = "/home/akhil/MS/DBMS/";
    private Map<String, String> tagMapping = new HashMap<>(); // contains id to tag name mapping
    private Map<String, Statistics> tagStatistics = new HashMap<>();
    public Phase1() {

        //createDemoNodes();
        nodes = XMLToIntervalTable.xmlToTreeConverter();
        //createDemoNodes();
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

        SystemDefs sysdef = new SystemDefs(dbpath, 100000, NUMBUF, "Clock");

        // creating the node table relation
        Tuple t = new Tuple();
        Tuple tForSortedFile = new Tuple();
        ProjectUtils.setTupleHeader(t);
        ProjectUtils.setTupleHeader(tForSortedFile);

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
        Heapfile hfSortedOnTag = null;
        try {
            hfSortedOnTag = new Heapfile("nodesSortedOnTag.in");
        } catch (Exception e) {
            System.err.println("*** error in Heapfile constructor ***");
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        tForSortedFile = new Tuple(size);
        ProjectUtils.setTupleHeader(t);
        ProjectUtils.setTupleHeader(tForSortedFile);
        Vector<NodeTable> nodesSortedBasedOnTag = new Vector<>(nodes);
        Collections.sort(nodesSortedBasedOnTag, new Comparator<NodeTable>() {
            @Override
            public int compare(NodeTable nodeTable, NodeTable t1) {
                return nodeTable.nodename.compareTo(t1.nodename);
            }
        });
        int numnodes = nodes.size();
        for (int i = 0; i < numnodes; i++) {
            System.out.println(i);
            try {
                IntervalType interval = ((NodeTable) nodes.elementAt(i)).interval;
                String nodeName = ((NodeTable) nodes.elementAt(i)).nodename;
                Statistics currStatistics = tagStatistics.get(nodeName);
                if (currStatistics != null) {
                    currStatistics.totalCount++;
                    currStatistics.intervalRange = (currStatistics.intervalRange
                            * ((double)(currStatistics.totalCount - 1) / currStatistics.totalCount))
                            + ((double) (interval.e - interval.s)) / currStatistics.totalCount;
                } else {
                    currStatistics = new Statistics(1, interval.e - interval.s);
                    tagStatistics.put(nodeName, currStatistics);
                }
                t.setIntervalFld(1, interval);
                tForSortedFile.setIntervalFld(1, ((NodeTable) nodesSortedBasedOnTag.elementAt(i)).interval);
                t.setStrFld(2, nodeName);
                tForSortedFile.setStrFld(2, ((NodeTable) nodesSortedBasedOnTag.elementAt(i)).nodename);
            } catch (Exception e) {
                System.err.println("*** Heapfile error in Tuple.setStrFld() ***");
                status = FAIL;
                e.printStackTrace();
            }

            try {
                byte[] ba = t.returnTupleByteArray();
                byte[] baForSortedFile = tForSortedFile.returnTupleByteArray();
                int c = ba.length;
                rid = f.insertRecord(ba);
                hfSortedOnTag.insertRecord(baForSortedFile);
            } catch (Exception e) {
                System.err.println("*** error in Heapfile.insertRecord() ***");
                status = FAIL;
                e.printStackTrace();
            }
        }
        f.freeNextFreeDirPage();

        ProjectUtils.createIndex(hfSortedOnTag, "nodeIndex.in");
        // ProjectUtils.testScan("nodes.in", "nodeIndex.in");
        System.out.println("BTreeIndex created successfully.\n");
        if (status != OK) {
            // bail out
            System.err.println("*** Error creating relation for nodes");
            Runtime.getRuntime().exit(1);
        }

        //creating b tree index on interval
        ProjectUtils.createIntervalIndex(f, t);
        
        ProjectUtils.createCompositeIndex(f, t);

        //querying b tree index on interval
        // ProjectUtils.doIntervalScan(1, 100, t);

    }


    private void testIndex() {

    }



    private void createDemoNodes() {
        nodes = new Vector<NodeTable>();
        nodes.addElement(new NodeTable("B", new IntervalType(2, 9, 2)));
        nodes.addElement(new NodeTable("C", new IntervalType(3, 6, 3)));
        nodes.addElement(new NodeTable("A", new IntervalType(1, 14, 1)));

        nodes.addElement(new NodeTable("B", new IntervalType(10, 11, 2)));
        nodes.addElement(new NodeTable("B", new IntervalType(12, 13, 2)));

        nodes.addElement(new NodeTable("D", new IntervalType(7, 8, 3)));
        nodes.addElement(new NodeTable("E", new IntervalType(4, 5, 4)));

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


    private void SortMergeCondition(CondExpr[] OutFilter, int offset) {
        OutFilter[0].next = null;
        OutFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
        OutFilter[0].type1 = new AttrType(AttrType.attrSymbol);
        OutFilter[0].type2 = new AttrType(AttrType.attrSymbol);
        OutFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), offset);
        OutFilter[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        OutFilter[0].flag =0;

    }

    private void checkNodeTree() {
        Map<String, Integer> countMap = new HashMap<>();
        for (NodeTable node : nodes) {
            if (node.nodename.equals("root"))
                countMap.put(node.nodename, 1);
            if (node.nodename.equals("Entry"))
                countMap.put(node.nodename, 1);
            if (node.nodename.equals("Ref"))
                countMap.put(node.nodename, 1);
            if (countMap.size() == 3)
                break;
        }

        if (countMap.size() < 3)
            System.out.println("Invalida node tree");
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

        if (!tagMapping.get(rule.innerTag).equals("*")) {	
            outFilter[1].next = null;
            outFilter[1].op = new AttrOperator(AttrOperator.aopEQ);
            outFilter[1].type1 = new AttrType(AttrType.attrSymbol);
            outFilter[1].type2 = new AttrType(AttrType.attrString);
            outFilter[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
            outFilter[1].operand2.string = tagMapping.get(rule.innerTag);
            outFilter[2] = null;        
        } else {
            outFilter[1] = null;
        }

        if (!tagMapping.get(rule.innerTag).equals("*")) {
            //Inner table comparison.
            rightFilter[0].next = null;
            rightFilter[0].op = new AttrOperator(AttrOperator.aopEQ);
            rightFilter[0].type1 = new AttrType(AttrType.attrSymbol);
            rightFilter[0].type2 = new AttrType(AttrType.attrString);
            rightFilter[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
            rightFilter[0].operand2.string = tagMapping.get(rule.innerTag);

            rightFilter[1] = null;
        } else {
            rightFilter[0] = null;
        }
    }

    private void populateNodeOffsetMap(Map<String, Integer> offsetMap, String nodeName, int nodeNumber) {
        offsetMap.put(nodeName, 2*nodeNumber);
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

        System.out.println("Created heap file for query");

    }
    
	public void compute(List<Rule> rules) {
		// Map containing the corresponding column number for the
		// given tag Id in the joined table.

		HashMap<String, Integer> tagOffsetMap = new HashMap<>();

		int nodeNumber = 1;
		boolean status = OK;

		Iterator initialScanner = null;

		AttrType[] baseTableAttrTypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = TAG_LENGTH;

		Rule firstRule = rules.get(0);
		CondExpr[] innerRelFilterConditions = ProjectUtils.getInitialCond(tagMapping.get(firstRule.outerTag));

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };

		try {
			if (tagMapping.get(firstRule.outerTag).equals("*")) {
				initialScanner = new FileScan("nodesSortedOnTag.in", baseTableAttrTypes, baseTableStringLengths,
						(short) 2, (short) 2, initialProjection, null);
			}
			initialScanner = new IndexScan(new IndexType(IndexType.B_Index), "nodesSortedOnTag.in", "nodeIndex.in",
					ProjectUtils.getNodeTableAttrType(), ProjectUtils.getNodeTableStringSizes(), 2, 2,
					ProjectUtils.getProjections(), innerRelFilterConditions, 2, false);
		} catch (Exception e) {
			status = FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}

		populateNodeOffsetMap(tagOffsetMap, firstRule.outerTag, nodeNumber);
		nodeNumber++;
		populateNodeOffsetMap(tagOffsetMap, firstRule.innerTag, nodeNumber);
		nodeNumber++;

		CondExpr[] filterConditions = new CondExpr[3];
		filterConditions[0] = new CondExpr();
		filterConditions[1] = new CondExpr();
		filterConditions[2] = new CondExpr();

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
					baseTableStringLengths, 10, initialScanner, "nodesSortedOnTag.in", filterConditions,
					innerRelFilterConditions, currProjection, 4,
					tagMapping.get(firstRule.outerTag).equals("*") ? "IntervalIndex.in" : "CompositeIndex.in", 1);
		} catch (Exception e) {
			System.err.println("*** Error preparing for nested_loop_join");
			System.err.println("" + e);
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}
		// Needs to iterate only from the second rule.
		int ruleNumber = 2;
		for (int x = 1; x < rules.size(); ++x) {
			Rule currRule = rules.get(x);
			if (!tagOffsetMap.containsKey(currRule.outerTag)) {
				// Technically, this should never happen, since the input tree is connected.
				populateNodeOffsetMap(tagOffsetMap, currRule.outerTag, nodeNumber);
				nodeNumber++;
			}

			if (!tagOffsetMap.containsKey(currRule.innerTag)) {
				populateNodeOffsetMap(tagOffsetMap, currRule.innerTag, nodeNumber);
				nodeNumber++;
			}

			String indexName = findIndex(currRule);

			filterConditions = new CondExpr[4];
			filterConditions[0] = new CondExpr();
			filterConditions[1] = new CondExpr();
			filterConditions[2] = new CondExpr();
			filterConditions[3] = new CondExpr();

			innerRelFilterConditions = new CondExpr[2];
			innerRelFilterConditions[0] = new CondExpr();
			innerRelFilterConditions[1] = new CondExpr();
			setConditions(filterConditions, innerRelFilterConditions, currRule, tagOffsetMap.get(currRule.outerTag),
					false);

			// After each rule the 2 more columns will be added.
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

			// Projection size will also increase by 2 in every rule. Also, an additional 2
			// more columns will
			// be added after the join with the inner table. This additional 2 columns will
			// be of the tag id
			// which has not occurred before in any of the rules. We can be assured that the
			// outer tag of a rule
			// will always be new since we have ordered the rules in a level wise fashion
			// and also because of
			// the assumption that there are no rules with a circular relationship.
			currProjection = new FldSpec[2 * ruleNumber + 2];
			for (int i = 0; i < 2 * ruleNumber; i++) {
				currProjection[i] = new FldSpec(new RelSpec(RelSpec.outer), i + 1);
			}
			currProjection[2 * ruleNumber] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
			currProjection[2 * ruleNumber + 1] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

			try {
				currIterator = new NestedLoopsJoins(joinedTableAttrTypes, 2 * ruleNumber, joinedTableStringLengths,
						baseTableAttrTypes, 2, baseTableStringLengths, 10, prevIterator,
						indexName.equals("nodeIndex.in") ? "nodesSortedOnTag.in" : "nodes.in", filterConditions,
						innerRelFilterConditions, currProjection, 2 * ruleNumber + 2, indexName,
						tagOffsetMap.get(currRule.outerTag) - 1);
			} catch (Exception e) {
				System.err.println("*** Error preparing for nested_loop_join");
				System.err.println("" + e);
				e.printStackTrace();
				Runtime.getRuntime().exit(1);
			}
			prevIterator = currIterator;
			ruleNumber++;

		}

		if (currIterator == null) {
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

		RID rid;
		Heapfile f = null;
		Tuple tup = null;
		try {
			f = new Heapfile("witness.in");
		} catch (Exception e) {
			System.err.println("*** error in Heapfile constructor ***");
			status = FAIL;
			e.printStackTrace();
		}
		try {
			int count = 1;
			if (queryPlanNumber == 1) {
				it1 = currIterator;
				if (wt1NoOfFlds == -1)
					wt1NoOfFlds = currIterator.get_next().noOfFlds();
				return; // since preserving the iterator
			}
			while ((finalTuple = currIterator.get_next()) != null) {
				System.out.println("Result " + count++ + ":");
				finalTuple.print(finalTupleAttrTypes);
				// complex query part
				if (wt2NoOfFlds == 0)
					wt2NoOfFlds = finalTuple.noOfFlds();
				if (queryPlanNumber == 2) {
					try {
						int b = finalTuple.getLength();
						tup = new Tuple(b);
						tup.tupleCopy(finalTuple);
						rid = f.insertRecord(tup.returnTupleByteArray());
					} catch (Exception e) {
						System.err.println("*** error in Heapfile.insertRecord() ***");
						status = FAIL;
						e.printStackTrace();
					}
				}
			}
			if (queryPlanNumber == 2)
				queryPlanNumber = 1;
			currIterator.close();
			prevIterator.close();
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
        int nodeNumber = 1;
        boolean status = OK;
        Iterator fileScan = null;
        AttrType[] Ntypes = {new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString)};
        short[] Nsizes = new short[1];
        Nsizes[0] = TAG_LENGTH;
        FldSpec[] Nprojection = {new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2)};
        FldSpec[] nljOutProj = {new FldSpec(new RelSpec(RelSpec.outer), 2), new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.innerRel), 2), new FldSpec(new RelSpec(RelSpec.innerRel), 1)};
        List<NestedLoopsJoins> listNLJ = new LinkedList<>();
        boolean isFirstRule = true;
        //takes ordered rules one by one perform nested loop join
        for (Rule rule : rules) {
			try {
				// CondExpr[] innerRelFilterConditions = new CondExpr[2];
				// innerRelFilterConditions[0] = new CondExpr();
				// innerRelFilterConditions[1] = new CondExpr();
				//
				// //inner filter conditions to restrict scan to evaluate with only filtered
				// nodes
				// innerRelFilterConditions[0].next = null;
				// innerRelFilterConditions[0].op = new AttrOperator(AttrOperator.aopEQ);
				// innerRelFilterConditions[0].type1 = new AttrType(AttrType.attrSymbol);
				// innerRelFilterConditions[0].type2 = new AttrType(AttrType.attrString);
				// innerRelFilterConditions[0].operand1.symbol = new FldSpec(new
				// RelSpec(RelSpec.outer), 2);
				// innerRelFilterConditions[0].operand2.string = tagMapping.get(rule.outerTag);
				//
				// innerRelFilterConditions[1] = null;
				CondExpr[] innerRelFilterConditions = ProjectUtils
						.getInitialCompositeCond(tagMapping.get(rule.outerTag));
				try {
					if (tagMapping.get(rule.outerTag).equals("*")) {
						fileScan = new FileScan("nodesSortedOnTag.in", ProjectUtils.getNodeTableAttrType(),
								ProjectUtils.getNodeTableStringSizes(), (short) 2, (short) 2,
								ProjectUtils.getProjections(), null);
					} else {
						fileScan = new IndexScan(new IndexType(IndexType.composite_Index), "nodesSortedOnTag.in",
								"CompositeIndex.in", ProjectUtils.getNodeTableAttrType(),
								ProjectUtils.getNodeTableStringSizes(), 2, 2, ProjectUtils.getProjections(),
								innerRelFilterConditions, 2, true);
					}
				} catch (Exception e) {
					status = GlobalProjectValues.FAIL;
					System.err.println("" + e);
					e.printStackTrace();
				}
				// fileScan = new FileScan("nodes.in", Ntypes, Nsizes, (short) 2, (short) 2,
				// Nprojection, innerRelFilterConditions);
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
				setConditions(outFilter, innerRelFilterConditions, rule, 1, true);
				inl = new NestedLoopsJoins(Ntypes, 2, Nsizes, Ntypes, 2, Nsizes, 10, fileScan, "nodesSortedOnTag.in",
						outFilter, innerRelFilterConditions, nljOutProj, 4,
						tagMapping.get(rule.innerTag).equals("*") ? "IntervalIndex.in" : "CompositeIndex.in", 1);
				if (!tagMapping.get(rule.outerTag).equals("*")) {
					inl.isOuterCompositeIndex = true;
				}
				isFirstRule = false;
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

                SortMergeCondition(outFilter, nodeOffsetMap.get(rules.get(x).outerTag));
                //This is fixed
                AttrType[] typesInner = {new AttrType(AttrType.attrString),
                        new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInterval)};
                short[] sizesInner= new short[2];
                sizesInner[0] = TAG_LENGTH;
                sizesInner[1] = TAG_LENGTH;
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
                //need to check if previously sorted tag is outer tag, if yes we don't have to sort outer,
                // if its ascending, its already sorted while inserting
                // No need to sort
                if(!prevSortedTag.equalsIgnoreCase(rules.get(x).outerTag)) {
                    sm = new SortMerge(Ntypes2, index * 2, Nsizes2,
                            typesInner, 4, sizesInner,
                            nodeOffsetMap.get(rules.get(x).outerTag), 4,
                            2, 4,
                            10,
                            prevSM, listNLJ.get(x),
                            order.tupleOrder == TupleOrder.Ascending ? true:false, order.tupleOrder == TupleOrder.Ascending ? true:false, order,
                            outFilter, proj2, 2 * index + 2);
                } else {
                    sm = new SortMerge(Ntypes2, index * 2, Nsizes2,
                            typesInner, 4, sizesInner,
                            nodeOffsetMap.get(rules.get(x).outerTag), 4,
                            2, 4,
                            10,
                            prevSM, listNLJ.get(x),
                            true, order.tupleOrder == TupleOrder.Ascending ? true:false, order,
                            outFilter, proj2, 2 * index + 2);
                }
                prevSortedTag = rules.get(x).outerTag;
                //cartesian product in minibase was not working as intended,
                //need to run more queries to see if the result matches NLJ
                sm.setCheckFlag(true);
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
//            int count = 1;
//            while ((t = prevSM.get_next()) != null) {
//                System.out.println("Result " + count++ + ":");
//                t.print(jtype);
//            }
            
//            ***************************************************
            int count = 1;
            if(queryPlanNumber == 1) {
            	it1 = prevSM;
            	if(prevSM != null && wt1NoOfFlds == -1) {
            		Tuple checktp = prevSM.get_next();
            		if(checktp != null )wt1NoOfFlds = checktp.noOfFlds();
            		return;
            	}
            	return;
            }
            
            RID rid;
            Heapfile f = null;
            Tuple tup = null;
            Tuple finalTuple = new Tuple();
            if(queryPlanNumber == 2)f = new Heapfile("witness.in");
            
                while ((finalTuple = prevSM.get_next()) != null) {
                    System.out.println("Result " + count++ + ":");
                    finalTuple.print(jtype);
                    //complex query part
                    if(queryPlanNumber == 2) {
                    	if(wt2NoOfFlds ==0 )wt2NoOfFlds = finalTuple.noOfFlds();
                        try {
                            int b = finalTuple.getLength();
                            tup = new Tuple(b);
                            tup.tupleCopy(finalTuple);
                            rid = f.insertRecord(tup.returnTupleByteArray());
                        } catch (Exception e) {
                            System.err.println("*** error in Heapfile.insertRecord() ***");
                            status = FAIL;
                            e.printStackTrace();
                        }
                    }
                }
                if(queryPlanNumber == 2) queryPlanNumber = 1;
                prevSM.close();
        } catch (Exception e) {
            System.err.println("*** Error preparing for get_next tuple");
            System.err.println("" + e);
            e.printStackTrace();
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
            System.err.println("readFailed: "+ e.getMessage());
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
				int relation = rule_components[2].equals("PC") ? Rule.RULE_TYPE_PARENT_CHILD
						: Rule.RULE_TYPE_ANCESTRAL_DESCENDENT;
				rankIndex[Integer.parseInt(rule_components[1]) - 1]++;
				ruleMap.get(Integer.parseInt(rule_components[0]))
						.add(new Rule(rule_components[0], rule_components[1], relation));
			}
			index++;
		}
		Integer root = getRoot(rankIndex);
		return getRulesDFS(root, ruleMap);
	}
    
    List<Rule> getRulesDFS(Integer root, Map<Integer, List<Rule>> ruleMap) {
        List<Rule>  orderedRules = new ArrayList<>();
        List<Rule> childRules = ruleMap.get(root);
        if (childRules != null) {
            getRulesDFSRec(childRules, ruleMap, orderedRules);
        }
        return orderedRules;
    }

    void getRulesDFSRec(List<Rule> rules, Map<Integer, List<Rule>> ruleMap,  List<Rule> orderedRules) {
        if(rules.size() == 0) return;
        for(Rule rule: rules) {
            orderedRules.add(rule);
            getRulesDFSRec(ruleMap.get(Integer.parseInt(rule.innerTag)), ruleMap, orderedRules);
        }
    }

    int queryPlanNumber = 0;
    Iterator it1 = null;
    Iterator it2 = null;
    String ruleFile1 = "temp1.in";
    String ruleFile2 = "temp2.in";
    String ruleFile  = "temp.in";
    int physOp = 0; // 1 for cp
    int wt2NoOfFlds = 0;
    int wt1NoOfFlds = -1;
    Tuple firstTmpTuple = null;

    private void complexPattern() {


        String choice = "Y";
        TaskFourUtils taskutils = null;
        System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
        Scanner scanner = new Scanner(System.in);
        List<Rule> rules1 = null;
        List<Rule> rules1Copy = null; String[] file_contents1 = null; String file1 = null;
        String file2 = null; String[] file_contents2 = null; List<Rule> rules2 = null;
        String[] chCOp = null; Tuple jtup = null;

        while (!choice.equals("N") && !choice.equals("n")) {
        	
        	wt1NoOfFlds = -1;
        	wt2NoOfFlds = 0;
        	it1 = null;
            BufMgr.page_access_counter = 0;

            if(scanner == null) scanner = new Scanner(System.in);
            int bufSize = 1000; //default
            try {
                String complexOperation = "";
                System.out.println("Enter the operation");
                complexOperation = scanner.nextLine();
                
                System.out.println("Enter first input filename for query");
                file1 = scanner.nextLine();
                file_contents1 = readFile(input_file_base + file1);
                rules1 = getRuleList(file_contents1);
                queryPlanNumber = 1;
                System.out.println("pattern tree 1 processing");
                rules1Copy = copyRules(rules1);
//              compute(rules1Copy);
//              compute(rules1);
                computeSM(rules1Copy, new TupleOrder(TupleOrder.Ascending));
                queryPlanNumber = 1;
                computeSM(rules1, new TupleOrder(TupleOrder.Ascending));
//              System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
                
                if(!complexOperation.contains("SRT") && !complexOperation.contains("GRP")) {
                	System.out.println("Enter second input filename for query");
                    file2 = scanner.nextLine();
                    file_contents2 = readFile(input_file_base + file2);
                    rules2 = getRuleList(file_contents2);
                    queryPlanNumber = 2;
                    computeSM(rules2, new TupleOrder(TupleOrder.Ascending));
                    System.out.println("pattern tree 2 processing");
//                  compute(rules2);
//                  System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
                }
                	taskutils = new TaskFourUtils(wt1NoOfFlds, wt2NoOfFlds);

                   		physOp = 1;

                    	chCOp = null;
                    	System.out.println("started processing complex query");

                        if(complexOperation.contains("CP")) {
                            taskutils.nestedLoop(it1);

                        }else if(complexOperation.contains("TJ")) {
                            chCOp = complexOperation.split(" ");
                            int i = Integer.parseInt(chCOp[1]);
                            int j = Integer.parseInt(chCOp[2]);
                            taskutils.nestedLoopNJOrTJ(it1, i, j, "TJ");

                        }else if(complexOperation.contains("NJ")) {
                            chCOp = complexOperation.split(" ");
                            int i = Integer.parseInt(chCOp[1]);
                            int j = Integer.parseInt(chCOp[2]);
                            taskutils.nestedLoopNJOrTJ(it1, i, j, "NJ");

                        }else if(complexOperation.contains("SRT")) {
                            chCOp = complexOperation.split(" ");
                            int i = Integer.parseInt(chCOp[1]);
                            System.out.println("Enter the buffer size");
                            bufSize = scanner.nextInt();
                            taskutils.sortPhysOP(it1, i, bufSize);

                        }else if(complexOperation.contains("GRP")) {
                            chCOp = complexOperation.split(" ");
                            int i = Integer.parseInt(chCOp[1]);
                            System.out.println("Enter the buffer size");
                            bufSize = scanner.nextInt();
                            taskutils.grpPhysOP(it1, i, bufSize);
                            jtup = null;
                            while((jtup = taskutils.get_next_GRP(i))!=null) {}
                        }
            }catch(Exception e) {
                e.printStackTrace();
            }
            finally {
            	taskutils = null;
			}
            taskutils = null;
            System.out.println("Press N to stop , Y to consider another pattern tree");
            choice = scanner.nextLine();
            try {
            	Heapfile f = new Heapfile("witness.in");
    			f.deleteFile();
    		} catch (Exception e1) {
    			// TODO Auto-generated catch block
    			e1.printStackTrace();
    		}
            if(it1 != null)
            {
            	try {
    				it1.close();
    			} catch (Exception e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
            	it1 = null;
            }
        }
        scanner.close();
    }

        private void input() {       String choice = "Y";

        while (!choice.equals("N") && !choice.equals("n")) {
            ProjectUtils.resetPageCounter();

            String[] file_contents = null;
            String file = null;
            Scanner scanner = new Scanner(System.in);
            do {
                System.out.println("Enter input filename for query");
                file = scanner.next();
                file_contents = readFile(input_file_base + file);
            }while(file_contents.length==1 && "".equals(file_contents[0]));

            List<Rule> rules = getRuleList(file_contents);
            //List<Rule> rules = getDemoRUles();
            // createQueryHeapFile("nodes.in", rules);
            long start = System.currentTimeMillis();
            Phase3 phase3 = new Phase3();
            System.out.println("Query Plan 1");
            phase3.IndexJoinWithTagIndex(tagMapping, rules);
            System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
            long timeTaken = (System.currentTimeMillis() - start)/1000;
            System.out.println("Time taken = " + timeTaken);
            start = System.currentTimeMillis();
            ProjectUtils.resetPageCounter();

            System.out.println("Query Plan 2");
            compute(rules);
            System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
            ProjectUtils.resetPageCounter();
            timeTaken = (System.currentTimeMillis() - start)/1000;
            System.out.println("Time taken = " + timeTaken);
            start = System.currentTimeMillis();

            System.out.println("Query Plan 3 (Sort Merge)");
            computeSM(rules, new TupleOrder(TupleOrder.Ascending));
            System.out.println("Number of page accessed = " + BufMgr.page_access_counter);
            ProjectUtils.resetPageCounter();
            System.out.println("Time taken = " + (System.currentTimeMillis() - start)/1000);


            System.out.println("Press N to stop");
            choice = scanner.next();
            System.out.print(choice);
        }
        System.out.println("OVERR!!!");}

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

    private List<Rule> getDemoRUles()  {
        tagMapping.put("1", "A");
        tagMapping.put("2", "B");
        tagMapping.put("3", "C");
        tagMapping.put("4", "D");
        tagMapping.put("4", "E");
        Rule rule1 = new Rule("1", "2", Rule.RULE_TYPE_PARENT_CHILD);
        Rule rule2 = new Rule("1", "4", Rule.RULE_TYPE_ANCESTRAL_DESCENDENT);
        Rule rule3 = new Rule("2", "3", Rule.RULE_TYPE_PARENT_CHILD);

        //Rule rule3 = new Rule("B", "D", Rule.RULE_TYPE_ANCESTRAL_DESCENDENT);
        ArrayList<Rule> rules = new ArrayList<>();
        rules.add(rule1);
        rules.add(rule2);
        rules.add(rule3);
        return rules;

    }

    public static List<Rule> copyRules(List<Rule> rule){
        List<Rule> resultRules = new ArrayList<>();

        for(Rule r : rule) {
            Rule newRule = new Rule(r.outerTag, r.innerTag, r.ruleType);
            resultRules.add(newRule);
        }
        return resultRules;
    }

    private Integer getRoot(int[] rankIndex) {
        for (int i = 0; i < rankIndex.length; i++)
            if (rankIndex[i] == 0)
                return i + 1;
        return -1;
    }

    private String findIndex(Rule rule) {
        Statistics statsOuter = tagStatistics.get(rule.outerTag);
        Statistics statsInner = tagStatistics.get(rule.innerTag);
        if (statsInner == null || statsOuter == null) {
            return "nodeIndex.in";
        }
        if (tagStatistics.get(rule.innerTag).equals("*")) {
        	return "IntervalIndex.in";
        }
        if (statsOuter.intervalRange < statsInner.totalCount) {
            return "IntervalIndex.in";
        }
        return "nodeIndex.in";
    }

    public void getMenu() {

//        phase1.input();
        Scanner scanner = new Scanner(System.in);
        String choice = "Y";
        while (!choice.equals("N") && !choice.equals("n")) {
            System.out.println("Enter 1. Query Plans 2. Complex Queries");
            int option = scanner.nextInt();
            scanner.nextLine();
            if (option == 1)
                input();
            else {
                complexPattern();
            }
            System.out.println("Press N to exit.");
            choice = scanner.nextLine();
        }
    }

    public static void main(String[] args) {
        Phase1 phase1 = new Phase1();

        phase1.input();
        phase1.getMenu();

        //phase1.compute();
        //phase1.computeSM();

    }
}