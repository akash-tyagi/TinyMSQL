package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.Stmt;

public class Expression implements Stmt {
	Term term;
	String op;
	Expression exp;

	@Override
	public void create(String query) {
		term = new Term();
		if (query.contains("+"))
			op = "+";
		else if (query.contains("-"))
			op = "-";
		else {
			term.create(query);
			return;
		}

		Pattern pattern = Pattern.compile("(.*)" + op + "(.*)");
		Matcher matcher = pattern.matcher(query);
		term.create(matcher.group(1));
		exp = new Expression();
		exp.create(matcher.group(2));
	}

}
