package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.StmtInterface;

public class CompPredicate {
	Expression exp1;
	char compOp;
	Expression exp2;

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
			System.out.println(
					"COMP PRED-->CompOp:" + compOp + " RAW EXPRESSIONS:" + matcher.group(1) + "," + matcher.group(2));
			exp1.create(matcher.group(1));
			exp2.create(matcher.group(2));
		} else {
			System.out.println("ERROR ::: COMP PREDICATE Invalid:" + query);
			System.exit(1);
		}
	}

	public boolean execute() {
		String res1 = exp1.execute();
		String res2 = exp2.execute();

		switch (compOp) {
		case '=':
			return res1 == res2;
		case '<':
			return Integer.getInteger(res1) < Integer.getInteger(res2);
		case '>':
			return Integer.getInteger(res1) > Integer.getInteger(res2);
		}
		System.out.println("ERROR ::: COMP PREDICATE Execution:" + res1 + " " + compOp + " " + res2);
		System.exit(1);
		return false;
	}

}
