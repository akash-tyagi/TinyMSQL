package database.physicalquery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import database.DbManager;
import database.GlobalVariable;
import storageManager.Schema;

public class JoinOptimization {
	DbManager dbManager;
	// List elements: 1. Size 2.Cost of permutation
	Map<String, List<Double>> permutations;
	public Map<List<String>, List<String>> join_columns_map;

	public JoinOptimization(DbManager dbManager) {
		this.dbManager = dbManager;
	}

	public String[] getLeftJoinOptimizedSequence(List<String> tables) {
		if (tables.size() == 1)
			return new String[] { tables.get(0) };
		permutations = new HashMap<>();
		getJoinPermutations(tables, 0);
		double minCost = Integer.MAX_VALUE;
		String minKey = "";
		// made a copy of keyset to avoid concurrentExcutionException
		Set<String> keys = new TreeSet<String>(permutations.keySet());
		for (String key : keys) {
			double cost = getJoinCost(key);
			if (cost < minCost) {
				minCost = cost;
				minKey = key;
			}
		}
		System.out.println("Join Optimization Best Order:" + minKey);
		return minKey.split(",");
	}

	private Double getJoinCost(String key) {
		if (permutations.get(key) != null)
			return permutations.get(key).get(1);
		String[] tables = key.split(",");
		List<Double> values = new ArrayList<>();

		// if key only has one table, return cost as 0 and update map
		if (tables.length == 1) {
			values.add(0.0);
			values.add(0.0);
			permutations.put(key, values);
			return 0.0;
		}
		List<String> tableList = Arrays.asList(tables);
		double cost = 0.0;
		for (int len = tables.length - 1; len > 0; len--) {
			String subKey = String.join(",", tableList.subList(0, len));
			getJoinCost(subKey);
			cost += permutations.get(subKey).get(0);
		}
		double size = getJoinSize(key);
		values.add(size);
		values.add(cost);
		permutations.put(key, values);

		if (GlobalVariable.isTestJoinOpt) {
			System.out.println(
					"JoinCostFor:" + key + " Cost:" + cost + " Size:" + size);
		}
		return cost;
	}

	// private int testCodeforOptimization(String table) {
	// if (table.equals("e"))
	// return 1000;
	// if (table.equals("f"))
	// return 2000;
	// if (table.equals("g"))
	// return 3000;
	// if (table.equals("h"))
	// return 4000;
	// return 0;
	// }

	private Double getJoinSize(String key) {
		String[] tables = key.split(",");
		Map<String, List<Integer>> attributeValues = new HashMap<>();
		double size = 1;
		for (String table : tables) {
			getAtrributeValues(table, attributeValues);
			size *= dbManager.schema_manager.getRelation(table)
					.getNumOfTuples();
			// size *= testCodeforOptimization(table);
		}
		// System.out.println("size:" + size);
		for (List<Integer> attrValues : attributeValues.values()) {
			if (attrValues.size() <= 1)
				continue;
			int min = attrValues.get(0);
			for (Integer value : attrValues) {
				// System.out.println("divide:" + value);
				size /= value;
				if (value < min)
					min = value;
			}
			// for each attribute A appearing at least twice, divide by all but
			// the least of the V( R , A ) ’s.
			// System.out.println("mul:" + min);
			size *= min;
		}
		return size;
	}

	private void getAtrributeValues(String table,
			Map<String, List<Integer>> attributeValues) {
		Schema schema = dbManager.schema_manager.getSchema(table);
		List<String> fieldNames = schema.getFieldNames();
		for (String fieldName : fieldNames) {
			Map<String, Integer> attrLevel3Map = dbManager.vTable.get(table)
					.get(fieldName);
			if (attrLevel3Map == null)
				continue;
			List<Integer> values = attributeValues.get(fieldName);
			if (values == null) {
				values = new ArrayList<Integer>();
				attributeValues.put(fieldName, values);
			}
			values.add(attrLevel3Map.size());
		}
	}

	// Expecting atleast 2 tables, otherwise will fail
	private void getJoinPermutations(List<String> str, int id) {
		if (id == str.size()) {
			Collections.swap(str, 0, 1);
			// removing same but permuted joins
			String key = String.join(",", str);
			if (permutations.containsKey(key)) {
				Collections.swap(str, 0, 1);
				return;
			}
			Collections.swap(str, 0, 1);
			key = String.join(",", str);
			permutations.put(key, null);
			return;
		}
		for (int i = id; i < str.size(); i++) {
			Collections.swap(str, i, id);
			getJoinPermutations(str, id + 1);
			Collections.swap(str, i, id);
		}
	}

	public boolean generateJoinColumns(String query, List<String> tables) {
		join_columns_map = new HashMap<>();
		if (query == null)
			return false;
		for (int i = 0; i < tables.size() - 1; i++) {
			for (int j = i + 1; j < tables.size(); j++) {
				String table1 = tables.get(i);
				String table2 = tables.get(j);
				Pattern pattern1;
				Pattern pattern2;
				if (!isJoinable(query, table1, table2))
					return false;
				List<String> field_names1 = dbManager.schema_manager
						.getRelation(table1).getSchema().getFieldNames();
				field_names1.retainAll(dbManager.schema_manager
						.getRelation(table2).getSchema().getFieldNames());
				List<String> join_columns = new ArrayList<>();
				for (String field_name : field_names1) {
					pattern1 = Pattern.compile(table1 + "\\." + field_name
							+ "\\s*=\\s*" + table2 + "\\." + field_name);
					pattern2 = Pattern.compile(table2 + "\\." + field_name
							+ "\\s*=\\s*" + table1 + "\\." + field_name);
					if (pattern1.matcher(query).find()
							|| pattern2.matcher(query).find()) {
						join_columns.add(field_name);
					}
				}
				if (join_columns.size() > 0) {
					List<String> key = new ArrayList<>();
					key.add(table1);
					key.add(table2);
					join_columns_map.put(key, join_columns);
					key = new ArrayList<>();
					key.add(table2);
					key.add(table1);
					join_columns_map.put(key, join_columns);
				}
			}
		}
		return true;
	}

	private boolean isJoinable(String query, String table1, String table2) {
		Pattern pattern1, pattern2;
		pattern1 = Pattern.compile(table1 + "\\.[a-z][a-z0-9]*" + "\\s*>\\s*"
				+ table2 + "\\.[a-z][a-z0-9]*");
		pattern2 = Pattern.compile(table2 + "\\.[a-z][a-z0-9]*" + "\\s*>\\s*"
				+ table1 + "\\.[a-z][a-z0-9]*");
		if (pattern1.matcher(query).find() || pattern2.matcher(query).find()) {
			return false;
		}

		pattern1 = Pattern.compile(table1 + "\\.[a-z][a-z0-9]*" + "\\s*<\\s*"
				+ table2 + "\\.[a-z][a-z0-9]*");
		pattern2 = Pattern.compile(table2 + "\\.[a-z][a-z0-9]*" + "\\s*<\\s*"
				+ table1 + "\\.[a-z][a-z0-9]*");
		if (pattern1.matcher(query).find() || pattern2.matcher(query).find()) {
			return false;
		}
		return true;
	}

	public List<String> getJoinColumns(String table1, String table2) {
		System.out.println(table1+" "+table2);
		String[] tables = table1.split("\\_");
		List<String> columns = new ArrayList<>();
		for (String table : tables) {
			List<String> relations = new ArrayList<>();
			relations.add(table);
			relations.add(table2);
			if (join_columns_map.get(relations) != null)
				columns.addAll(join_columns_map.get(relations));
		}
		return (columns.size() > 0) ? columns : null;
	}

	public void printJoinColumns() {
		System.out.println("\nJoinable Columns");
		for (Map.Entry<List<String>, List<String>> entry : join_columns_map
				.entrySet()) {
			for (String table : entry.getKey()) {
				System.out.print(table + ",");
			}
			System.out.print(":");
			for (String column : entry.getValue()) {
				System.out.println(column + ",");
			}
		}
	}

}
