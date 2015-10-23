package database.parser;

import java.util.regex.Pattern;

public class DropStmt extends Stmt {
	String tableName;

	public void create(String query) {
		pattern = Pattern.compile("DROP TABLE ([a-z][a-z0-9]*)");
		matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			System.out.println("DROP Statement: TableName:" + tableName);
		} else {
			System.out.println("DROP Statement: Invalid:" + query);
			System.exit(1);
		}
	}

}
