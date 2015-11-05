package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;

import database.GlobalVariable;
import database.parser.StmtInterface;

public class SearchCond implements StmtInterface {
	BoolTerm boolTerm;
	SearchCond cond;

	@Override
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

		// while (query.contains("OR")) {
		// int pos = query.indexOf("OR");
		// int pos1 = query.indexOf('[');
		// int pos2 = query.indexOf(']');
		//
		// if (pos1 != -1 && pos1 < pos && pos2 > pos) {
		// query = query.substring(pos2 + 1);
		// } else if ((pos1 != -1 && pos1 < pos && pos2 < pos) || pos1 == -1) {
		// index = pos;
		// break;
		// } else if (pos1 != -1 && pos < pos1) {
		// index = pos;
		// break;
		// }
		// }
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

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}
}
