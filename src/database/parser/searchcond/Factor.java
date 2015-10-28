package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.Stmt;

public class Factor implements Stmt {
	String colName;
	String literal;
	int integer;
	Expression exp;

	@Override
	public void create(String query) {
		if (query.contains("(")) {
			Pattern pattern = Pattern.compile("\\s*[(](.*)[)]\\s*");
			Matcher matcher = pattern.matcher(query);
			if (matcher.find()) {
				exp = new Expression();
				exp.create(matcher.group(1));
			} else {
				System.out.println("ERROR::: Factor Invalid stmt:" + query);
			}
		} else {
			Pattern pattern = Pattern.compile("\\s*(.*)\\s*");
			Matcher matcher = pattern.matcher(query);
			if (matcher.find()) {
				String temp = matcher.group(1);
				if (temp.charAt(0) >= '0' && temp.charAt(0) <= '9')
					integer = Integer.parseInt(temp);
				else if (query.contains("\""))
					literal = temp;
				else
					colName = temp;
				System.out.println("FACTOR int:" + integer + " literal:"
						+ literal + " colName:" + colName);

			} else {
				System.out.println("ERROR ::: FACTOR Invalid:" + query);
				System.exit(1);
			}
		}
	}
}
