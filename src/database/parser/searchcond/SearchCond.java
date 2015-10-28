package database.parser.searchcond;

import database.GlobalVariable;
import database.parser.Stmt;

public class SearchCond implements Stmt {
	BoolTerm boolTerm;
	SearchCond cond;

	@Override
	public void create(String query) {
		String rawBoolTerm = query;
		int index = -1;

		while (query.contains("OR")) {
			int pos = query.indexOf("OR");
			int pos1 = query.indexOf('[');
			int pos2 = query.indexOf(']');

			if (pos1 != -1 && pos1 < pos && pos2 > pos) {
				query = query.substring(pos2 + 1);
			} else if ((pos1 != -1 && pos1 < pos && pos2 < pos) || pos1 == -1) {
				index = pos;
				break;
			} else if (pos1 != -1 && pos < pos1) {
				index = pos;
				break;
			}
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
}
