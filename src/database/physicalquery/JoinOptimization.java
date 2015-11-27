package database.physicalquery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import database.DbManager;
import database.GlobalVariable;
import storageManager.Schema;

public class JoinOptimization {
	DbManager dbManager;
	// List elements: 1. Size 2.Cost of permutation
	Map<String, List<Double>> permutations;

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
}
