package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.StmtInterface;

public class Term {
	Factor factor;
	Term term;
	char op;

	public void create(String query) {
		factor = new Factor();
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
		if (query2.contains("*")) {
			op = '*';
			index = indexList.get(query2.indexOf('*'));
		} else if (query2.contains("/")) {
			op = '/';
			index = indexList.get(query2.indexOf('/'));
		} else {
			System.out.println("Term--> rawFactor:" + query);
			factor.create(query);
			return;
		}
		System.out.println("Term--> op:" + op + " rawFactor:" + query.substring(0, index - 1) + " rawTerm:"
				+ query.substring(index + 1));
		factor.create(query.substring(0, index - 1));
		term = new Term();
		term.create(query.substring(index + 1));
	}

	public String execute() {
		if (op == '*') {
			return String.valueOf(Integer.getInteger(factor.execute()) * Integer.getInteger(term.execute()));
		} else if (op == '/') {
			return String.valueOf(Integer.getInteger(factor.execute()) / Integer.getInteger(term.execute()));
		}
		return factor.execute();// TODO
	}
}
