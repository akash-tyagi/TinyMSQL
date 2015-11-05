package database.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.GlobalVariable;
import database.Manager;

public class Parser {

	public StmtInterface stmt;
	Manager manager;

	public Parser(Manager manager) {
		this.manager = manager;
	}

	public void parse(String query) {
		Pattern pattern = Pattern.compile("(DROP|CREATE|DELETE|INSERT|SELECT)");
		Matcher matcher = pattern.matcher(query);
		if (!matcher.find())
			System.out.println("XX Parser Statement: Invalid");
		if (GlobalVariable.isTest)
			System.out.println("$$ Parser: Statement Type:" + matcher.group(1));

		switch (matcher.group(1)) {
		case "CREATE":
			stmt = new CreateStmt(manager);
			stmt.create(query);
			break;
		case "DELETE":
			stmt = new DeleteStmt(manager);
			stmt.create(query);
			break;
		case "DROP":
			stmt = new DropStmt(manager);
			stmt.create(query);
			break;
		case "INSERT":
			stmt = new InsertStmt(manager);
			stmt.create(query);
			break;
		case "SELECT":
			stmt = new SelectStmt(manager);
			stmt.create(query);
			break;
		}
	}
}
