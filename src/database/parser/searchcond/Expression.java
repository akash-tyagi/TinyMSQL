package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;

import storageManager.Tuple;

public class Expression {
	Term term;
	char op;
	Expression exp;

	public void create(String query) {
		term = new Term();

		List<Integer> factorList = new ArrayList<Integer>();
		int j = 0;
		for (int i = 0; i < query.length(); i++) {
			char ch = query.charAt(i);
			if (ch == '(')
				j++;
			else if (ch == ')')
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

		int index = 0;
		if (query2.contains("+")) {
			op = '+';
			index = indexList.get(query2.indexOf('+'));
		} else if (query2.contains("-")) {
			op = '-';
			index = indexList.get(query2.indexOf('-'));
		} else {
			System.out.println("Expression--> rawTerm:" + query);
			term.create(query);
			return;
		}

		System.out.println("Expression--> op:" + op + " rawTerm:"
				+ query.substring(0, index - 1) + " rawExp:"
				+ query.substring(index + 1));
		term.create(query.substring(0, index - 1));
		exp = new Expression();
		exp.create(query.substring(index + 1));
	}

	public String execute(Tuple tuple) {
		String str1 = term.execute(tuple);
		String str2 = "";
		if (exp != null)
			str2 = exp.execute(tuple);
		System.out.println("EXPRESSION EXECUTE op:" + op + " val1:" + str1
				+ " val2:" + str2);
		if (op == '+') {
			return String.valueOf(
					Integer.getInteger(str1) + Integer.getInteger(str2));
		} else if (op == '-') {
			return String.valueOf(
					Integer.getInteger(str1) - Integer.getInteger(str2));
		}
		return term.execute(tuple);
	}
}
