package database.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CreateStmt implements Stmt {
	class AttrDataTypePair {
		public AttrDataTypePair(String group1, String group2) {
			attrType = group1;
			dataType = group2;
		}

		String attrType;
		String dataType;
	};

	String tableName;
	List<AttrDataTypePair> attrDataTypePairList;

	public CreateStmt() {
		this.attrDataTypePairList = new ArrayList<AttrDataTypePair>();
	}

	private void parseAttibuteList(String attrList) {
		Pattern pattern = Pattern
				.compile("([a-z][a-z0-9]*)\\s*(INT|STR20)\\s*(,*\\s*.*)");
		Matcher matcher = pattern.matcher(attrList);

		while (matcher.find()) {
			String attrName = matcher.group(1);
			String dataType = matcher.group(2);
			if (attrName.isEmpty() || dataType.isEmpty()) {
				System.out
						.println("ERROR ::: CREATE statement: Invalid Attributes:"
								+ attrList);
				System.exit(1);
			}
			AttrDataTypePair pair = new AttrDataTypePair(attrName, dataType);
			attrDataTypePairList.add(pair);
			System.out.println("CREATE Statement: AttrName:" + attrName
					+ " AttrType:" + dataType);
			attrList = matcher.group(3);
			matcher = pattern.matcher(attrList);
		}
	}

	public void create(String query) {
		Pattern pattern = Pattern
				.compile("CREATE TABLE ([a-z][a-z0-9]*)\\s*[(]\\s*(.*)\\s*[)]");
		Matcher matcher = pattern.matcher(query);
		if (matcher.find()) {
			tableName = matcher.group(1);
			String attrList = matcher.group(2);
			System.out.println("CREATE Statement: TableName:" + tableName);
			System.out.println("CREATE Statement: AttrList:" + attrList);
			parseAttibuteList(attrList);
		} else {
			System.out.println("ERROR ::: CREATE statement: Invalid:" + query);
			System.exit(1);
		}
	}

}
