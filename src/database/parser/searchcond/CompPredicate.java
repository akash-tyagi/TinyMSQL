package database.parser.searchcond;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.GlobalVariable;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class CompPredicate {
	Expression exp1;
	char compOp;
	Expression exp2;
	// boolean isSelectionOptmized = false;

	public CompPredicate() {

	}

	public CompPredicate(Expression exp1, Expression exp2, char compOp) {
		this.exp1 = exp1;
		this.exp2 = exp2;
		this.compOp = compOp;
	}

	public void create(String query) {
		if (query.contains("<"))
			compOp = '<';
		else if (query.contains(">"))
			compOp = '>';
		else
			compOp = '=';
		Pattern pattern = Pattern.compile("(.*)" + compOp + "(.*)");
		Matcher matcher = pattern.matcher(query);
		exp1 = new Expression();
		exp2 = new Expression();
		if (matcher.find()) {
			if (GlobalVariable.isTestParsing)
				System.out.println(
						"COMP PRED-->CompOp:" + compOp + " RAW EXPRESSIONS:"
								+ matcher.group(1) + "," + matcher.group(2));
			exp1.create(matcher.group(1));
			exp2.create(matcher.group(2));
		} else {
			System.out.println("ERROR ::: COMP PREDICATE Invalid:" + query);
			System.exit(1);
		}
	}

	public boolean execute(Tuple tuple) {
		String res1 = exp1.execute(tuple);
		String res2 = exp2.execute(tuple);
		if (GlobalVariable.isTestExecution)
			System.out.println("COMP PREDICATE Execution:" + res1 + " " + compOp
					+ " " + res2);
		switch (compOp) {
		case '=':
			return res1.equals(res2);
		case '<':
			return Integer.parseInt(res1) < Integer.parseInt(res2);
		case '>':
			return Integer.parseInt(res1) > Integer.parseInt(res2);
		}
		if (GlobalVariable.isTestExecution)
			System.out.println("ERROR ::: COMP PREDICATE Execution:" + res1
					+ " " + compOp + " " + res2);
		System.exit(1);
		return false;
	}

	public CompPredicate getSelectionCond(List<Relation> relations) {
		if (!(exp1.isSelectionOptimizable(relations)
				&& exp2.isSelectionOptimizable(relations)))
			return null;
		return new CompPredicate(exp1, exp2, compOp);

	}

	public void print() {
		exp1.print();
		if (compOp == '=')
			System.out.print(" = ");
		else if (compOp == '>')
			System.out.print(" > ");
		else
			System.out.print(" < ");
		exp2.print();
	}

}
