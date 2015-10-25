package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.Stmt;

public class CompPredicate implements Stmt {
	Expression exp1;
	String compOp;
	Expression exp2;

	@Override
	public void create(String query) {
		if (query.contains("<"))
			compOp = "<";
		else if (query.contains(">"))
			compOp = ">";
		else
			compOp = "=";
		Pattern pattern = Pattern.compile("(.*)" + compOp + "(.*)");
		Matcher matcher = pattern.matcher(query);
		exp1 = new Expression();
		exp2 = new Expression();
		exp1.create(matcher.group(1));
		exp2.create(matcher.group(2));
	}

}
