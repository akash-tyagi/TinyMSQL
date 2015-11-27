package database.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.GlobalVariable;
import database.DbManager;
import database.utils.GeneralUtils;

public class Parser {

	public StmtInterface stmt;
	DbManager dbManager;

	public Parser(DbManager dbManager) {
		this.dbManager = dbManager;
	}

	public StmtInterface parse(String query) {
		GeneralUtils.cleanMainMemory(dbManager.mem);
		Pattern pattern = Pattern.compile("(DROP|CREATE|DELETE|INSERT|SELECT)");
		Matcher matcher = pattern.matcher(query);
		if (!matcher.find())
			System.out.println("Parser: Statement Invalid:" + query);
		if (GlobalVariable.isTestExecution)
			System.out.println("$$ Parser: Statement Type:" + matcher.group(1));

		switch (matcher.group(1)) {
		case "CREATE":
			stmt = new CreateStmt(dbManager);
			stmt.create(query);
			break;
		case "DELETE":
			stmt = new DeleteStmt(dbManager);
			stmt.create(query);
			break;
		case "DROP":
			stmt = new DropStmt(dbManager);
			stmt.create(query);
			break;
		case "INSERT":
			stmt = new InsertStmt(dbManager);
			stmt.create(query);
			break;
		case "SELECT":
			stmt = new SelectStmt(dbManager);
			stmt.create(query);
			break;
		}
		return stmt;
	}
}
