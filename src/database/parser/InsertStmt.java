package database.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertStmt implements Stmt {
	String tableName;
	List<String> attrList;

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
		Pattern pattern = Pattern
				.compile("INSERT INTO ([a-z][a-z0-9]*)\\s*[(]\\s*(.*)\\s*[)]\\s*(.*)");
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			String attrList = matcher.group(2);
			String tuples = matcher.group(3);
			System.out.println("CREATE Statement: TableName:" + tableName);
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
