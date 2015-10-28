package database.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertStmt implements Stmt {
	String tableName;
	List<String> attrList;
	Stmt selectStmt;

	public InsertStmt() {
		attrList = new ArrayList<String>();
	}

	private void parseAttibuteList(String attrList2) {
		Pattern pattern = Pattern.compile("([a-z][a-z0-9]*)\\s*(,*\\s*.*)");
		Matcher matcher = pattern.matcher(attrList2);

		while (matcher.find()) {
			String attrName = matcher.group(1);
			attrList.add(attrName);
			System.out.println("CREATE Statement: AttrName:" + attrName);
			attrList2 = matcher.group(2);
			matcher = pattern.matcher(attrList2);
		}
	}

	private void parseTuples(String tuples) {
		if (tuples.contains("SELECT")) {
			selectStmt = new SelectStmt();
			selectStmt.create(tuples);
			return;
		}

		Pattern pattern = Pattern.compile("([a-z][a-z0-9]*)\\s*(,*\\s*.*)");
		Matcher matcher = pattern.matcher(tuples);

		while (matcher.find()) {
			String attrName = matcher.group(1);
			attrList.add(attrName);
			System.out.println("CREATE Statement: AttrName:" + attrName);
			tuples = matcher.group(2);
			matcher = pattern.matcher(tuples);
		}
	}

	@Override
	public void create(String query) {
		String attrList = "", tuples = "";
		Pattern pattern = Pattern
				.compile("INSERT INTO ([a-z][a-z0-9]*)\\s*(.*)");
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			System.out.println("CREATE Statement: TableName:" + tableName);
			String rawList = matcher.group(2);
			if (rawList.contains("VALUES")) {
				pattern = Pattern
						.compile("\\s*[(]\\s*(.*)\\s*[)]\\s*VALUES\\s*[(]\\s*(.*)\\s*[)]");
				matcher = pattern.matcher(rawList);
				if (matcher.find()) {
					attrList = matcher.group(1);
					tuples = matcher.group(2);
				}
			} else {
				pattern = Pattern
						.compile("\\s*[(]\\s*(.*)\\s*[)]\\s*(SELECT .*)");
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
}
