package database.parser.searchcond;

import java.util.ArrayList;
import java.util.List;

import database.GlobalVariable;
import storageManager.Relation;
import storageManager.Schema;
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
			if (GlobalVariable.isTestParsing)
				System.out.println("Expression--> rawTerm:" + query);
			term.create(query);
			return;
		}
		if (GlobalVariable.isTestParsing)
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
		if (GlobalVariable.isTestExecution)
			System.out.println("EXPRESSION EXECUTE op:" + op + " val1:" + str1
					+ " val2:" + str2);
		if (op == '+') {
			return String
					.valueOf(Integer.parseInt(str1) + Integer.parseInt(str2));
		} else if (op == '-') {
			return String
					.valueOf(Integer.parseInt(str1) - Integer.parseInt(str2));
		}
		return str1;
	}

	public boolean isSelectionOptimizable(List<Relation> relations) {
		if (exp == null) {
			return term.isSelectionOptimizable(relations);
		}
		return exp.isSelectionOptimizable(relations)
				&& term.isSelectionOptimizable(relations);

	}

	public void print() {
		term.print();
		if (exp == null)
			return;
		if (op == '+')
			System.out.print(" + ");
		else
			System.out.print(" - ");
		exp.print();
	}
}
