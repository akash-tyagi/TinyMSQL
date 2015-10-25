package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.Stmt;

public class Term implements Stmt {
	Factor factor;
	Term term;
	String op;

	@Override
	public void create(String query) {
		factor = new Factor();
		if (query.contains("*"))
			op = "*";
		else if (query.contains("/"))
			op = "/";
		else {
			factor.create(query);
			return;
		}

		Pattern pattern = Pattern.compile("(.*)" + op + "(.*)");
		Matcher matcher = pattern.matcher(query);
		factor.create(matcher.group(1));
		term = new Term();
		term.create(matcher.group(2));
	}

}
