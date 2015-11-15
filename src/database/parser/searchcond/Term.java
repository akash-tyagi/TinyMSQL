package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;

import database.GlobalVariable;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

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
			if (GlobalVariable.isTestParsing)
				System.out.println("Term--> rawFactor:" + query);
			factor.create(query);
			return;
		}
		if (GlobalVariable.isTestParsing)
			System.out.println("Term--> op:" + op + " rawFactor:"
					+ query.substring(0, index - 1) + " rawTerm:"
					+ query.substring(index + 1));
		factor.create(query.substring(0, index - 1));
		term = new Term();
		term.create(query.substring(index + 1));
	}

	public String execute(Tuple tuple) {
		String str1 = factor.execute(tuple);
		String str2 = "";
		if (term != null)
			str2 = term.execute(tuple);
		if (GlobalVariable.isTestExecution)
			System.out.println("TERM EXECUTE op:" + op + " val1:" + str1
					+ " val2:" + str2);
		if (op == '*') {
			return String
					.valueOf(Integer.parseInt(str1) * Integer.parseInt(str2));
		} else if (op == '/') {
			return String
					.valueOf(Integer.parseInt(str1) / Integer.parseInt(str2));
		}
		return str1;// TODO
	}

	public boolean isSelectionOptimizable(List<Relation> relations) {
		if (term == null)
			return factor.isSelectionOptimizable(relations);
		return factor.isSelectionOptimizable(relations)
				&& term.isSelectionOptimizable(relations);
	}

	public void print() {
		factor.print();
		if (term == null)
			return;
		if (op == '*')
			System.out.print(" * ");
		else
			System.out.print(" / ");
		term.print();
	}
}
