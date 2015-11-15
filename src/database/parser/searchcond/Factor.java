package database.parser.searchcond;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.GlobalVariable;
import storageManager.Relation;
import storageManager.Schema;
import storageManager.Tuple;

public class Factor {
	String colName;
	String literal;
	int integer;
	Expression exp;

	public void create(String query) {
		if (query.contains("(")) {
			Pattern pattern = Pattern.compile("\\s*[(](.*)[)]\\s*");
			Matcher matcher = pattern.matcher(query);
			if (matcher.find()) {
				if (GlobalVariable.isTestParsing)
					System.out.println("Factor-->Exp:" + matcher.group(1));
				exp = new Expression();
				exp.create(matcher.group(1));
			} else {
				System.out.println("ERROR::: Factor Invalid stmt:" + query);
			}
		} else {
			query = query.trim();
			// TODO : may be I dont need this code
			if (query.charAt(0) >= '0' && query.charAt(0) <= '9')
				integer = Integer.parseInt(query);
			else if (query.contains("\""))
				literal = query;
			else
				colName = query;
			if (GlobalVariable.isTestParsing)
				System.out.println("FACTOR int:" + integer + " literal:"
						+ literal + " colName:" + colName);

		}
	}

	public String execute(Tuple tuple) {
		if (colName != null) {
			// find the right field name
			for (String field_name : tuple.getSchema().getFieldNames()) {
				if (field_name.contains(colName)) {
					if (GlobalVariable.isTestExecution)
						System.out.println("FACTOR found colName" + colName
								+ " FieldName:" + field_name + " FieldValue:"
								+ tuple.getField(field_name).toString());
					return tuple.getField(field_name).toString();
				}
			}
			System.out.println(
					"ERROR Factor couldnt find the field,colName:" + colName);
			System.out.print("Schema:" + tuple.getSchema());
			System.exit(1);
			return "";
		} else if (literal != null)
			return literal;
		else if (exp != null)
			return exp.execute(tuple);
		return Integer.toString(integer);
	}

	public boolean isSelectionOptimizable(List<Relation> relations) {
		if (exp != null)
			return exp.isSelectionOptimizable(relations);
		if (colName == null) {
			return true;
		}
		for (Relation relation : relations) {
			String relName = relation.getRelationName();
			Schema schema = relation.getSchema();
			// If column is not of same table
			if (colName.contains(".")
					&& colName.split("\\.")[0].equals(relName) == false)
				return false;
			for (String fieldName : schema.getFieldNames())
				if (colName.contains(fieldName)) {
					return true;
				}
		}
		return false;
	}

	public void print() {
		if (exp != null) {
			exp.print();
			return;
		}
		if (colName != null)
			System.out.print("(" + colName + ")");
		else if (literal != null)
			System.out.print("(" + literal + ")");
		else
			System.out.print("(" + integer + ")");
	}
}
