package project;

import global.AttrOperator;
import global.AttrType;
import global.IndexType;
import global.RID;
import heap.Heapfile;
import heap.Tuple;
import index.IndexScan;
import iterator.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Phase3 {

	int queryPlanNumber = 0;
	Iterator it1 = null;
	Iterator it2 = null;
	String ruleFile1 = "temp1.in";
	String ruleFile2 = "temp2.in";
	String ruleFile = "temp.in";
	int physOp = 0; // 1 for cp
	int wt2NoOfFlds = 0;
	int wt1NoOfFlds = -1;
	Tuple firstTmpTuple = null;

	public void IndexJoinWithTagIndex(Map<String, String> tagMapping, List<Rule> rules) {
		// Map containing the corresponding column number for the
		// given tag Id in the joined table.

		HashMap<String, Integer> tagOffsetMap = new HashMap<>();

		int nodeNumber = 1;
		boolean status = GlobalProjectValues.OK;

		Iterator fileScanner = null;

		AttrType[] baseTableAttrTypes = { new AttrType(AttrType.attrInterval), new AttrType(AttrType.attrString) };
		short[] baseTableStringLengths = new short[1];
		baseTableStringLengths[0] = GlobalProjectValues.TAG_LENGTH;

		Rule firstRule = rules.get(0);

		CondExpr[] innerRelFilterConditions = new CondExpr[3];
		innerRelFilterConditions[0] = new CondExpr();
		innerRelFilterConditions[1] = new CondExpr();
		innerRelFilterConditions[2] = new CondExpr();

		// Inner table comparison.
		innerRelFilterConditions[0].next = null;
		innerRelFilterConditions[0].op = new AttrOperator(AttrOperator.aopEQ);
		innerRelFilterConditions[0].type1 = new AttrType(AttrType.attrSymbol);
		innerRelFilterConditions[0].type2 = new AttrType(AttrType.attrString);
		innerRelFilterConditions[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		innerRelFilterConditions[0].operand2.string = tagMapping.get(firstRule.outerTag);

		innerRelFilterConditions[1].next = null;
		innerRelFilterConditions[1].op = new AttrOperator(AttrOperator.aopEQ);
		innerRelFilterConditions[1].type1 = new AttrType(AttrType.attrSymbol);
		innerRelFilterConditions[1].type2 = new AttrType(AttrType.attrString);
		innerRelFilterConditions[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);
		innerRelFilterConditions[1].operand2.string = tagMapping.get(firstRule.outerTag);

		innerRelFilterConditions[2] = null;

		FldSpec[] initialProjection = { new FldSpec(new RelSpec(RelSpec.outer), 1),
				new FldSpec(new RelSpec(RelSpec.outer), 2) };
		try {

			fileScanner = new IndexScan(new IndexType(IndexType.B_Index), "nodesSortedOnTag.in", "nodeIndex.in",
					ProjectUtils.getNodeTableAttrType(), ProjectUtils.getNodeTableStringSizes(), 2, 2,
					ProjectUtils.getProjections(), innerRelFilterConditions, 2, false);
		} catch (Exception e) {
			status = GlobalProjectValues.FAIL;
			System.err.println("" + e);
			e.printStackTrace();
		}

		ProjectUtils.populateNodeOffsetMap(tagOffsetMap, firstRule.outerTag, nodeNumber);
		nodeNumber++;
		ProjectUtils.populateNodeOffsetMap(tagOffsetMap, firstRule.innerTag, nodeNumber);
		nodeNumber++;

		CondExpr[] filterConditions = new CondExpr[4];
		filterConditions[0] = new CondExpr();
		filterConditions[1] = new CondExpr();
		filterConditions[2] = new CondExpr();
		filterConditions[3] = new CondExpr();

		innerRelFilterConditions = new CondExpr[2];
		innerRelFilterConditions[0] = new CondExpr();
		innerRelFilterConditions[1] = new CondExpr();

		setConditions(filterConditions, innerRelFilterConditions, firstRule, 1, true, tagMapping);
		FldSpec[] currProjection = { new FldSpec(new RelSpec(RelSpec.outer), 2),
				new FldSpec(new RelSpec(RelSpec.outer), 1), new FldSpec(new RelSpec(RelSpec.innerRel), 2),
				new FldSpec(new RelSpec(RelSpec.innerRel), 1) };

		NestedLoopsJoins prevIterator = null;
		NestedLoopsJoins currIterator = null;
		try {
			prevIterator = new NestedLoopsJoins(baseTableAttrTypes, 2, baseTableStringLengths, baseTableAttrTypes, 2,
					baseTableStringLengths, 10, fileScanner, "nodesSortedOnTag.in", filterConditions,
					innerRelFilterConditions, currProjection, 4, "CompositeIndex.in", 1);
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
				ProjectUtils.populateNodeOffsetMap(tagOffsetMap, currRule.outerTag, nodeNumber);
				nodeNumber++;
			}

			if (!tagOffsetMap.containsKey(currRule.innerTag)) {
				ProjectUtils.populateNodeOffsetMap(tagOffsetMap, currRule.innerTag, nodeNumber);
				nodeNumber++;
			}

			String indexName = "CompositeIndex.in";

			filterConditions = new CondExpr[4];
			filterConditions[0] = new CondExpr();
			filterConditions[1] = new CondExpr();
			filterConditions[2] = new CondExpr();
			filterConditions[3] = new CondExpr();

			innerRelFilterConditions = new CondExpr[2];
			innerRelFilterConditions[0] = new CondExpr();
			innerRelFilterConditions[1] = new CondExpr();
			setConditions(filterConditions, innerRelFilterConditions, currRule, tagOffsetMap.get(currRule.outerTag),
					false, tagMapping);

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
				joinedTableStringLengths[i] = GlobalProjectValues.TAG_LENGTH;
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
						indexName.equals("nodeIndex.in") || indexName.equals("CompositeIndex.in")
								? "nodesSortedOnTag.in"
								: "nodes.in",
						filterConditions, innerRelFilterConditions, currProjection, 2 * ruleNumber + 2, indexName,
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
			status = GlobalProjectValues.FAIL;
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
						status = GlobalProjectValues.FAIL;
						e.printStackTrace();
					}
				}
			}
			if (queryPlanNumber == 2)
				queryPlanNumber = 1;
		} catch (Exception e) {
			System.err.println("*** Error preparing for get_next tuple");
			System.err.println("" + e);
			Runtime.getRuntime().exit(1);
		}

		if (status != GlobalProjectValues.OK) {
			System.out.println(" Error Occured !!");
		}

	}

	private void setConditions(CondExpr[] outFilter, CondExpr[] rightFilter, Rule rule, int offset, boolean isFirstRule,
			Map<String, String> tagMapping) {
		
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
//		
//		if (!tagMapping.get(rule.innerTag).equals("*")) {
//			outFilter[2].next = null;
//			outFilter[2].op = new AttrOperator(AttrOperator.aopEQ);
//			outFilter[2].type1 = new AttrType(AttrType.attrSymbol);
//			outFilter[2].type2 = new AttrType(AttrType.attrString);
//			outFilter[2].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
//			outFilter[2].operand2.string = tagMapping.get(rule.innerTag);
//
//			outFilter[3] = null;
//		} else {
//			outFilter[2] = null;
//		}
		
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

}
