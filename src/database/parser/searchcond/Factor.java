package database.parser.searchcond;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.parser.StmtInterface;
import storageManager.Tuple;

public class Factor {
	String colName;
	String literal;
	int integer;
	Expression exp;

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
			// TODO : may be I dont need this code
			if (query.charAt(0) >= '0' && query.charAt(0) <= '9')
				integer = Integer.parseInt(query);
			else if (query.contains("\""))
				literal = query;
			else
				colName = query;
			System.out.println("FACTOR int:" + integer + " literal:" + literal + " colName:" + colName);

		}
	}

	public String execute(Tuple tuple) {
		if (colName != null)
			return tuple.getField(colName).toString();
		else if (literal != null)
			return literal;
		else if (exp != null)
			return exp.execute();
		return Integer.toString(integer);
	}
}
