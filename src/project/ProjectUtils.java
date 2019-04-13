package project;

import btree.BTreeFile;
import btree.StringKey;
import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.RID;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import index.IndexScan;
import iterator.CondExpr;
import iterator.FldSpec;
import iterator.RelSpec;
import org.w3c.dom.Attr;

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


    public static short[] getNodeTableStringSizes() {
        short[] nodeTableStringSizes = new short[1];
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
}
