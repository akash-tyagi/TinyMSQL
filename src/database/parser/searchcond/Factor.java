package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.Stmt;

public class Factor implements Stmt {
	String name;
	Expression exp;

	@Override
	public void create(String query) {
		if (query.contains("(")) {
			Pattern pattern = Pattern.compile("\\s*[(](.*)[)]\\s*");
			Matcher matcher = pattern.matcher(query);
			exp = new Expression();
			exp.create(matcher.group(1));
		} else {
			Pattern pattern = Pattern.compile("\\s*([a-z0-9]*)\\s*");
			Matcher matcher = pattern.matcher(query);
			name = matcher.group(1);
		}
	}
}
