package database.physicalquery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import database.DbManager;
import storageManager.Schema;

public class JoinOptimization {
	DbManager dbManager;
	// List elements: 1. Size 2.Cost of permutation
	Map<String, List<Double>> permutations;

	public JoinOptimization(DbManager dbManager) {
		this.dbManager = dbManager;
	}

	public JoinOptimization() {

	}

	public String[] getLeftJoinOptimizedSequence(List<String> tables) {
		permutations = new HashMap<>();
		printPermutations(tables, 0);
		double min = Integer.MAX_VALUE;
		String minKey = "";
		for (Map.Entry<String, List<Double>> entry : permutations.entrySet()) {
			System.out.println(entry.getKey() + "->" + entry.getValue());
			double temp = getJoinCost(entry.getKey());
			if (temp < min) {
				min = temp;
				minKey = entry.getKey();
			}
		}
		return minKey.split(",");
	}

	private Double getJoinCost(String key) {
		if (permutations.containsKey(key))
			return permutations.get(key).get(1);
		String[] tables = key.split(",");
		List<Double> values = new ArrayList<>();
		// if key only has one table, return cost as 0 and update map
		if (tables.length == 1) {

			double size = dbManager.schema_manager.getRelation(key)
					.getNumOfTuples();
			values.add(size);
			// cost will be 0
			values.add(0.0);
			permutations.put(key, values);
			return 0.0;
		}
		List<String> tableList = Arrays.asList(tables);
		double cost = 0.0;
		for (int len = tables.length - 1; len >= 2; len--)
			cost += getJoinCost(String.join(",", tableList.subList(0, len)));
		values.add(getJoinSize(key));
		values.add(cost);
		permutations.put(key, values);
		return cost;
	}

	private Double getJoinSize(String key) {
		String[] tables = key.split(",");
		Map<String, List<Integer>> attributeValues = new HashMap<>();
		double size = 1;
		for (String table : tables) {
			getAtrributeValues(table, attributeValues);
			size *= dbManager.schema_manager.getRelation(table)
					.getNumOfTuples();
		}
		for (List<Integer> attrValues : attributeValues.values()) {
			if (attrValues.size() <= 1)
				continue;
			int min = attrValues.get(0);
			for (Integer value : attrValues) {
				size /= value;
				if (value < min)
					min = value;
			}
			// for each attribute A appearing at least twice, divide by all but
			// the least of the V( R , A ) ’s.
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
	private void printPermutations(List<String> str, int id) {
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
			System.out.println(str + " Key:" + key);
			return;
		}

		for (int i = id; i < str.size(); i++) {
			Collections.swap(str, i, id);
			printPermutations(str, id + 1);
			Collections.swap(str, i, id);
		}
	}
}
