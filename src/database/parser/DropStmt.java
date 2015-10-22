package database.parser;

import java.util.regex.Pattern;

public class DropStmt extends Stmt {
	String tableName;

	public DropStmt() {
		pattern = Pattern.compile("DROP TABLE ([a-z][a-z0-9]*)");
	}

	public void create(String query) {
		matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			System.out.println("Drop Statement: TableName:" + tableName);
		} else {
			System.out.println("Drop statement: Invalid");
			System.exit(1);
		}
	}

}
