package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.StmtInterface;

public class Factor implements StmtInterface {
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
				System.out.println("Factor-->Exp:" + matcher.group(1));
				exp = new Expression();
				exp.create(matcher.group(1));
			} else {
				System.out.println("ERROR::: Factor Invalid stmt:" + query);
			}
		} else {
			query = query.trim();
			if (query.charAt(0) >= '0' && query.charAt(0) <= '9')
				integer = Integer.parseInt(query);
			else if (query.contains("\""))
				literal = query;
			else
				colName = query;
			System.out.println("FACTOR int:" + integer + " literal:" + literal
					+ " colName:" + colName);

		}
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}
}
