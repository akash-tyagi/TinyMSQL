package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;

import database.GlobalVariable;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class BoolTerm {
	BoolTerm boolTerm;
	BoolFactor boolFactor;

	public BoolTerm() {
	}

	public BoolTerm(BoolFactor boolFactor, BoolTerm boolTerm) {
		this.boolFactor = boolFactor;
		this.boolTerm = boolTerm;
	}

	public BoolTerm(BoolFactor boolFactor) {
		this.boolFactor = boolFactor;
	}

	public BoolTerm(BoolTerm boolTerm) {
		this.boolTerm = boolTerm;
	}

	public void create(String query) {
		String rawBoolFactor = query;

		List<Integer> factorList = new ArrayList<Integer>();
		int j = 0;
		for (int i = 0; i < query.length(); i++) {
			char ch = query.charAt(i);
			if (ch == '[')
				j++;
			else if (ch == ']')
				j--;
			factorList.add(j);
		}

		List<Integer> indexList = new ArrayList<Integer>();
		String query2 = "";
		for (int i = 0; i < factorList.size(); i++) {
			if (factorList.get(i) == 0) {
				indexList.add(i);
				query2 += query.charAt(i);
			}
		}
		int index = -1;
		if (query2.contains("AND")) {
			index = indexList.get(query2.indexOf("AND"));
			if (GlobalVariable.isTestExecution)
				System.out.println("BOOLTERM-->RAWBOOL TERM:"
						+ query.substring(index + 4));
			boolTerm = new BoolTerm();
			boolTerm.create(query.substring(index + 4));
			rawBoolFactor = query.substring(0, index - 1);
		}

		// if (rawBoolFactor.contains("AND")) {
		// Pattern pattern = Pattern.compile("(.*) AND (.*)");
		// Matcher matcher = pattern.matcher(rawBoolFactor);
		// if (matcher.find()) {
		// rawBoolFactor = matcher.group(1);
		// System.out.println("BOOLTERM-->RAWBOOL TERM:"
		// + matcher.group(2));
		// boolTerm = new BoolTerm();
		// boolTerm.create(matcher.group(2));
		// } else {
		// System.out.println("ERROR ::: BOOLTERM Invalid:" + query);
		// System.exit(1);
		// }
		// }
		if (GlobalVariable.isTestExecution)
			System.out.println("BOOLTERM-->RAWBOOL FACTOR:" + rawBoolFactor);
		boolFactor = new BoolFactor();
		boolFactor.create(rawBoolFactor);
	}

	public BoolTerm getSelectionCond(List<Relation> relations) {
		BoolFactor bFactor = null;
		BoolTerm bTerm = null;
		if (boolFactor != null)
			bFactor = boolFactor.getSelectionCond(relations);
		if (boolTerm != null)
			bTerm = boolTerm.getSelectionCond(relations);

		if (bFactor != null && bTerm != null)
			return new BoolTerm(bFactor, bTerm);
		else if (bFactor != null)
			return new BoolTerm(bFactor);
		else if (bTerm != null)
			return new BoolTerm(bTerm);
		return null;
	}

	public boolean execute(Tuple tuple) {
		boolean res = true;
		if (boolFactor != null)
			res = boolFactor.execute(tuple);
		// factor and term have AND condition
		if (res == true && boolTerm != null)
			res = boolTerm.execute(tuple);
		return res;
	}

	public void print() {
		System.out.print("[");
		if (boolFactor != null)
			boolFactor.print();
		if (boolTerm == null) {
			System.out.print("]");
			return;
		}
		if (!(boolTerm == null || boolFactor == null))
			System.out.print("] AND [");
		boolTerm.print();
		System.out.print("]");
	}
}
