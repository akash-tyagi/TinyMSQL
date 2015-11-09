package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;

import database.GlobalVariable;
import database.parser.StmtInterface;
import storageManager.Tuple;

public class SearchCond {
	BoolTerm boolTerm;
	SearchCond cond;

	public void create(String query) {

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
			if (GlobalVariable.isTest)
				System.out.println("SEARCHCOND-->RAWBOOL TERM:"
						+ rawBoolTerm.substring(0, index - 1) + " SearchCond:"
						+ rawBoolTerm.substring(index + 2));
			cond = new SearchCond();
			cond.create(rawBoolTerm.substring(index + 2));
			rawBoolTerm = rawBoolTerm.substring(0, index - 1);
		}
		if (GlobalVariable.isTest)
			System.out.println("SEARCHCOND-->RAWBOOL TERM:" + rawBoolTerm);
		boolTerm = new BoolTerm();
		boolTerm.create(rawBoolTerm);
	}

	public boolean execute(Tuple tuple) {
		boolean res = boolTerm.execute(tuple);
		if (res == false && cond != null)
			res = cond.execute(tuple);
		return res;
	}
}
