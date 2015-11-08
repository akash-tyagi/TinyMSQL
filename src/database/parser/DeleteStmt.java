package database.parser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.DbManager;
import database.parser.searchcond.SearchCond;

public class DeleteStmt extends StmtBase implements StmtInterface {
	String tableName;
	SearchCond cond;

	public DeleteStmt(DbManager manager) {
		super(manager);
	}

	@Override
	public void create(String query) {
		Pattern pattern = Pattern.compile("DELETE FROM (.*)");
		int type = 0;
		if (query.contains("WHERE")) {
			pattern = Pattern
					.compile("DELETE FROM\\s*([a-z][a-z0-9]*)\\s* WHERE (.*)");
			type = 1;
		}
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			System.out.println("DELETE Statement: TableName:" + tableName);
			if (type == 1) {
				cond = new SearchCond();
				cond.create(matcher.group(2));
			}
		} else {
			System.out.println("ERROR ::: DELETE statement: Invalid:" + query);
			System.exit(1);
		}

	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}

}
