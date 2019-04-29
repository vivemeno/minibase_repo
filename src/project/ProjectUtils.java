package project;

import btree.BTreeFile;
import btree.StringKey;
import global.AttrOperator;
import global.AttrType;
import global.CompositeType;
import global.GlobalConst;
import global.IndexType;
import global.IntervalType;
import global.RID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import intervalTree.IntervalKey;
import intervalTree.IntervalTreeFile;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import org.w3c.dom.Attr;

import java.util.Map;
import java.util.concurrent.locks.Condition;

public class ProjectUtils {
    public static final int STR_KEY_SIZE = 7;
    public static final int STR_FIELD_INDEX = 2;
    public static final int INTERVAL_FIELD_INDEX = 1;

    public static void createIndex(Heapfile f, String fileName) {
        Scan scan = null;
        try {
            scan = new Scan(f);
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }

        // create the index file
        BTreeFile btf = null;
        Tuple t = new Tuple();
        setTupleHeader(t);

        try {
            btf = new BTreeFile(fileName, AttrType.attrString, STR_KEY_SIZE, 1/*delete*/);
        }
        catch (Exception e) {
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }



        RID rid = new RID();
        String key = null;
        Tuple temp = null;
        try {
            temp = scan.getNext(rid);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        while ( temp != null) {
            t.tupleCopy(temp);

            try {
                key = t.getStrFld(STR_FIELD_INDEX);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            try {
                btf.insert(new StringKey(key), rid);
            }
            catch (Exception e) {
                e.printStackTrace();
            }

            try {
                temp = scan.getNext(rid);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        // close the file scan
        scan.closescan();

    }

    public static void populateNodeOffsetMap(Map<String, Integer> offsetMap, String nodeName, int nodeNumber) {
		offsetMap.put(nodeName, 2*nodeNumber);
	}
    
    public static void setTupleHeader(Tuple t) {
        AttrType[] nodeTableAttrTypes = getNodeTableAttrType();
        short[] nodeTableStringSizes = getNodeTableStringSizes();
        try {
            t.setHdr((short) 2, nodeTableAttrTypes, nodeTableStringSizes);
        } catch (Exception e) {
            System.err.println("*** error in Tuple.setHdr() ***");
            e.printStackTrace();
        }
    }

    public static AttrType[] getNodeTableAttrType() {
        AttrType[] nodeTableAttrTypes = new AttrType[2];
        nodeTableAttrTypes[0] = new AttrType(AttrType.attrInterval);
        nodeTableAttrTypes[1] = new AttrType(AttrType.attrString);
        return nodeTableAttrTypes;
    }
    
    public static CondExpr[] setIntervalIndexCond(IntervalType interval) {
    	CondExpr[] expr = new CondExpr[3];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopGE);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrInterval);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand2.interval = new IntervalType(interval.s, interval.s, 2);
		expr[0].next = null;
//	    expr[1] = null;

		expr[1] = new CondExpr();
		expr[1].op = new AttrOperator(AttrOperator.aopLE);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrInterval);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[1].operand2.interval = new IntervalType(interval.e, interval.e, 2);
		expr[1].next = null;
		expr[2] = null;
		return expr;
    }
    
    public static CondExpr[] setCompositeIndexCond(IntervalType interval, String nodeName) {
    	CondExpr[] expr = new CondExpr[3];
		expr[0] = new CondExpr();
		expr[0].op = new AttrOperator(AttrOperator.aopGE);
		expr[0].type1 = new AttrType(AttrType.attrSymbol);
		expr[0].type2 = new AttrType(AttrType.attrComposite);
		expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[0].operand2.composite = new CompositeType(new IntervalType (interval.s, 0, 0), nodeName);
		expr[0].next = null;

		expr[1] = new CondExpr();
		expr[1].op = new AttrOperator(AttrOperator.aopLE);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrComposite);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[1].operand2.composite = new CompositeType(new IntervalType (interval.e, 0, 0), nodeName);
		expr[1].next = null;
		expr[2] = null;
		return expr;
    }


    public static short[] getNodeTableStringSizes() {
        short[] nodeTableStringSizes = new short[2];
        nodeTableStringSizes[0] = STR_KEY_SIZE;
        return nodeTableStringSizes;
    }

    public static FldSpec[] getProjections() {
        FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2) };
        return initialProjection;
    }

    private static CondExpr[] getConditions() {
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
        expr[0].operand2.string = "A";
        expr[0].next = null;
        expr[1] = null;
        return expr;
    }

    public static void testScan(String fileName, String indexName ) {
        IndexScan iscan = null;
        AttrType[] attrType = getNodeTableAttrType();


        try {
            iscan = new IndexScan(new IndexType(IndexType.B_Index), fileName, indexName, attrType, getNodeTableStringSizes(), 2, 2, getProjections(), getConditions(), 2, true);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        Tuple t = null;
        String outval = null;
        System.out.println("Index test result \n");
        do {
            try {
                t = iscan.get_next();
                if (t != null)
                    System.out.print(t.getStrFld(STR_FIELD_INDEX));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } while(t != null);
    }
    
    public static void createIntervalIndex(Heapfile f, Tuple t) {
    	
    	// create an scan on the heapfile
		Scan scan = null;Tuple temp = null;

		// creating the node table relation
		AttrType[] nodeTableAttrTypes = new AttrType[2];
		nodeTableAttrTypes[0] = new AttrType(AttrType.attrInterval);
		nodeTableAttrTypes[1] = new AttrType(AttrType.attrString);

		short[] nodeTableStringSizes = new short[1];
		nodeTableStringSizes[0] = 5;
		
		try {
			scan = new Scan(f);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// create the index file on the integer field
		IntervalTreeFile btfInterval = null;
		try {
			btfInterval = new IntervalTreeFile("IntervalIndex.in", 1/* delete */);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		RID rid = new RID();
		IntervalType intervalType = null;
		String s = null;
		temp = null;

		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		int c =0 ;
		while (temp != null) {
			t.tupleCopy(temp);

			try {

				s = t.getStrFld(2);
				intervalType = t.getIntervalField(1);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
//				if(s==null || c == 784) {
//					System.out.println(s);
//				}
//				System.out.println(c++);
				btfInterval.insert(new IntervalKey(intervalType, s), rid);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				temp = scan.getNext(rid);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// close the file scan
		scan.closescan();

		System.out.println("BTreeIndex file insertion successfully completed.\n");
    	
    }
    
 public static void createCompositeIndex(Heapfile f, Tuple t) {
    	
    	// create an scan on the heapfile
		Scan scan = null;Tuple temp = null;

		// creating the node table relation
		AttrType[] nodeTableAttrTypes = new AttrType[2];
		nodeTableAttrTypes[0] = new AttrType(AttrType.attrInterval);
		nodeTableAttrTypes[1] = new AttrType(AttrType.attrString);

		short[] nodeTableStringSizes = new short[1];
		nodeTableStringSizes[0] = 5;
		
		try {
			scan = new Scan(f);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		// create the index file on the integer field
		compositeTree.IntervalTreeFile btfInterval = null;
		try {
			btfInterval = new compositeTree.IntervalTreeFile("CompositeIndex.in", 1/* delete */);
		} catch (Exception e) {
			e.printStackTrace();
			Runtime.getRuntime().exit(1);
		}

		RID rid = new RID();
		IntervalType intervalType = null;
		String s = null;
		temp = null;

		try {
			temp = scan.getNext(rid);
		} catch (Exception e) {
			e.printStackTrace();
		}
		int c =0 ;
		while (temp != null) {
			t.tupleCopy(temp);

			try {

				s = t.getStrFld(2);
				intervalType = t.getIntervalField(1);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
//				if(s==null || c == 784) {
//					System.out.println(s);
//				}
//				System.out.println(c++);
				btfInterval.insert(new compositeTree.IntervalKey(intervalType, s), rid);
			} catch (Exception e) {
				e.printStackTrace();
			}

			try {
				temp = scan.getNext(rid);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// close the file scan
		scan.closescan();

		System.out.println("BTreeIndex file insertion successfully completed.\n");
    	
    }
    
    public static void doIntervalScan(int initialStartValue, int endStartValue, Tuple t) {
    	
    	long startTime = System.nanoTime();

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
		expr[0].operand2.interval = new IntervalType(initialStartValue, 1, 2);
		expr[0].next = null;
//	    expr[1] = null;

		expr[1] = new CondExpr();
		expr[1].op = new AttrOperator(AttrOperator.aopLE);
		expr[1].type1 = new AttrType(AttrType.attrSymbol);
		expr[1].type2 = new AttrType(AttrType.attrInterval);
		expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);
		expr[1].operand2.interval = new IntervalType(endStartValue, 908, 2);
		expr[1].next = null;
		expr[2] = null;

		// creating the node table relation
		AttrType[] nodeTableAttrTypes = new AttrType[2];
		nodeTableAttrTypes[0] = new AttrType(AttrType.attrInterval);
		nodeTableAttrTypes[1] = new AttrType(AttrType.attrString);

		short[] nodeTableStringSizes = new short[1];
		nodeTableStringSizes[0] = 5;
		
		// start index scan
		boolean isIndexOnly = true;
		IndexScan iscan = null;
		try {
			iscan = new IndexScan(new IndexType(IndexType.interval_Index), "nodes.in", "IntervalIndex.in",
					nodeTableAttrTypes, nodeTableStringSizes, 2, 2, projlist, expr, 1, isIndexOnly);
		} catch (Exception e) {
			e.printStackTrace();
		}

		t = null;
		IntervalType iout = null;
		IntervalKey ioutKey = null;
		int ival = 100, count = 0; // low key

		try {
			t = iscan.get_nextInterval();
		} catch (Exception e) {
			e.printStackTrace();
		}

		while (t != null) {
			try {
				ioutKey = t.getCompositeField(1);
				++count;
//				System.out.println("count " + (count));
			} catch (Exception e) {
				e.printStackTrace();
			}

	      System.out.println("result "+ ioutKey.toString());

			try {
				t = iscan.get_nextInterval();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		long endTime = System.nanoTime();
		long duration = (endTime - startTime);
		
		System.err.println("total results : " + count);
		System.out.println("total time taken in ms : " + duration/1000000);
		// clean up
		try {
			iscan.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
    }
}
