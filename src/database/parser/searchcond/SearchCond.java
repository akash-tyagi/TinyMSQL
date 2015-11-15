package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;

import database.GlobalVariable;
import database.parser.StmtInterface;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class SearchCond {
	BoolTerm boolTerm;
	SearchCond cond;

	public SearchCond() {
	}

	public SearchCond(BoolTerm boolterm) {
		this.boolTerm = boolterm;
		cond = null;
	}

	public void create(String query) {
		if (GlobalVariable.isTestParsing)
			System.out.println("\n\n SEARCH CONDITION ----------------");
		String rawBoolTerm = query;
		int index = -1;

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

		if (query2.contains("OR")) {
			index = indexList.get(query2.indexOf("OR"));
		}

		if (index != -1) {
			if (GlobalVariable.isTestExecution)
				System.out.println("SEARCHCOND-->RAWBOOL TERM:"
						+ rawBoolTerm.substring(0, index - 1) + " SearchCond:"
						+ rawBoolTerm.substring(index + 2));
			cond = new SearchCond();
			cond.create(rawBoolTerm.substring(index + 2));
			rawBoolTerm = rawBoolTerm.substring(0, index - 1);
		}
		if (GlobalVariable.isTestExecution)
			System.out.println("SEARCHCOND-->RAWBOOL TERM:" + rawBoolTerm);
		boolTerm = new BoolTerm();
		boolTerm.create(rawBoolTerm);
	}

	public SearchCond getSelectionCond(List<Relation> relations) {
		// If OR cond on search then entire condition needs to be satisfied, not
		// optimizing for OR as union will be needed
		if (cond != null)
			return null;
		BoolTerm bTerm = boolTerm.getSelectionCond(relations);
		if (bTerm != null)
			return new SearchCond(boolTerm.getSelectionCond(relations));
		return null;
	}

	public boolean execute(Tuple tuple) {
		// currently bool term can not be null with cond not null
		// check for future condition
		// if (boolTerm == null)
		// return true;
		boolean res = boolTerm.execute(tuple);
		if (res == false && cond != null)
			res = cond.execute(tuple);
		return res;
	}

	public void print() {
		System.out.print("[");
		boolTerm.print();
		if (cond != null) {
			System.out.print("] OR [");
			cond.print();
		}
		System.out.print("]");
	}
}
