package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.StmtInterface;

public class Expression implements StmtInterface {
	Term term;
	String op;
	Expression exp;

	@Override
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
			op = "+";
			index = indexList.get(query2.indexOf('+'));
		} else if (query2.contains("-")) {
			op = "-";
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

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}
}
