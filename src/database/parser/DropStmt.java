package database.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.GlobalVariable;

public class DropStmt implements Stmt {
	String tableName;

	public void create(String query) {
		Pattern pattern = Pattern.compile("DROP\\s*TABLE\\s*([a-z][a-z0-9]*)\\s*");
		Matcher matcher = pattern.matcher(query);
		if (!matcher.find())
			System.out.println("XX DROP Statement: Invalid");
		tableName = matcher.group(1);

		if (GlobalVariable.isTest)
			System.out.println("$$ DROP Statement: TableName:" + tableName);
	}

}
