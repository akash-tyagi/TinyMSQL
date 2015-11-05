package database.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.Manager;

public class InsertStmt extends StmtBase implements StmtInterface {
	String tableName;
	List<String> attrList;
	StmtInterface selectStmt;

	public InsertStmt(Manager manager) {
		super(manager);
		attrList = new ArrayList<String>();
	}

	private void parseAttibuteList(String attrList2) {
		Pattern pattern = Pattern.compile("([a-z][a-z0-9]*)\\s*(,*\\s*.*)");
		Matcher matcher = pattern.matcher(attrList2);
		System.out.println("CREATE Statement: PARSING Attribute List");
		while (matcher.find()) {
			String attrName = matcher.group(1);
			attrName = attrName.trim();
			attrList.add(attrName);
			System.out.println("CREATE Statement: AttrName:" + attrName);
			attrList2 = matcher.group(2);
			matcher = pattern.matcher(attrList2);
		}
	}

	private void parseTuples(String tuples) {
		if (tuples.contains("SELECT")) {
			selectStmt = new SelectStmt(manager);
			selectStmt.create(tuples);
			return;
		}
		System.out.println("CREATE Statement: PARSING Value List");
		String[] list = tuples.split(",");
		for (String attrName : list) {
			attrName = attrName.trim();
			attrName = attrName.replaceAll("\"", "");
			attrList.add(attrName);
			System.out.println("CREATE Statement: AttrName:" + attrName);
		}
	}

	@Override
	public void create(String query) {
		String attrList = "", tuples = "";
		Pattern pattern = Pattern.compile("INSERT INTO ([a-z][a-z0-9]*)(.*)");
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			System.out.println("CREATE Statement: TableName:" + tableName);
			String rawList = matcher.group(2);
			if (rawList.contains("VALUES")) {
				pattern = Pattern
						.compile("\\s*[(](.*)[)]\\s*VALUES\\s*[(](.*)[)]");
				matcher = pattern.matcher(rawList);
				if (matcher.find()) {
					attrList = matcher.group(1);
					tuples = matcher.group(2);
				}
			} else {
				pattern = Pattern.compile("\\s*[(](.*)[)]\\s*(SELECT .*)");
				matcher = pattern.matcher(rawList);
				if (matcher.find()) {
					attrList = matcher.group(1);
					tuples = matcher.group(2);
				}
			}
			System.out.println("CREATE Statement: AttrList:" + attrList
					+ " Tuples:" + tuples);
			parseAttibuteList(attrList);
			parseTuples(tuples);
		} else {
			System.out.println("ERROR ::: CREATE statement: Invalid:" + query);
			System.exit(1);
		}
	}

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		
	}
}
