package database.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

	public Stmt stmt;
	public String query;
	Pattern pattern;
	Matcher matcher;

	public Parser(String query) {
		this.query = query;
		pattern = Pattern.compile("^(DROP|CREATE|DELETE|INSERT|SELECT)");
	}

	public void parse() {
		matcher = pattern.matcher(query);
		if (!matcher.find()) {
			System.out.println("Statement: Invalid");
			System.exit(1);
		}

		System.out.println("Statement Type Valid:" + matcher.group(1));
		switch (matcher.group(1)) {
		case "CREATE":
			stmt = new CreateStmt();
			stmt.create(query);
			break;
		case "DELETE":
			stmt = new DeleteStmt();
			stmt.create(query);
			break;
		case "DROP":
			stmt = new DropStmt();
			stmt.create(query);
			break;
		case "INSERT":
			stmt = new InsertStmt();
			stmt.create(query);
			break;
		case "SELECT":
			stmt = new SelectStmt();
			stmt.create(query);
			break;
		}
	}
}
