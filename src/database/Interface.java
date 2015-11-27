package database;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import database.logicalquerytree.LogicalQuery;
import database.parser.Parser;
import database.parser.SelectStmt;
import database.parser.StmtInterface;
import database.physicalquery.JoinOptimization;

public class Interface {
	ArrayList<String> queries;
	DbManager manager;

	public Interface() {
		queries = new ArrayList<>();
		manager = new DbManager();
	}

	/* Read single query from console */
	public void readText() throws IOException {
		BufferedReader br = new BufferedReader(
				new InputStreamReader(System.in));
		System.out.print("Enter Query:");
		String q = br.readLine();
		queries.add(q);
	}

	/* Read queries from text */
	public void readFile(String file) throws IOException {
		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = br.readLine()) != null) {
				queries.add(line);
			}
		}
	}

	public void parseQueries() {
		Parser parser = new Parser(manager);
		for (String query : queries) {
			if (query.contains("#"))
				continue;
			System.out.println("--------------------Parsing Query:" + query
					+ "-------------");
			StmtInterface stmt = parser.parse(query);
			// if (stmt instanceof SelectStmt) {
			// LogicalQuery logicalQuery = new LogicalQuery((SelectStmt) stmt);
			// }
		}
	}

	public static void main(String[] args) throws IOException {
		Interface iface = new Interface();
		iface.readFile("src/testQueries");
		iface.parseQueries();

		// DO NOT DELETE
		// TESTING CODE FOR JOIN OPTMIZATION
		// JoinOptimization jOptimization = new JoinOptimization(iface.manager);
		// List<String> tables = new ArrayList<>();
		// tables.add("e");
		// tables.add("f");
		// tables.add("g");
		// tables.add("h");
		// HashMap<String, HashMap<String, HashMap<String, Integer>>> vTable =
		// iface.manager.vTable;
		//
		// for (int i = 0; i < 1000; i++) {
		// vTable.get("e").get("a").put("" + i, i);
		// if (i < 500) {
		// vTable.get("g").get("d").put("" + i, i);
		// }
		// if (i < 400) {
		// vTable.get("h").get("d").put("" + i, i);
		// }
		// if (i < 300) {
		// vTable.get("g").get("c").put("" + i, i);
		// }
		// if (i < 200) {
		// vTable.get("f").get("d").put("" + i, i);
		// }
		// if (i < 100) {
		// vTable.get("f").get("b").put("" + i, i);
		// vTable.get("h").get("c").put("" + i, i);
		// }
		// if (i < 50) {
		// vTable.get("e").get("b").put("" + i, i);
		// vTable.get("f").get("a").put("" + i, i);
		// vTable.get("g").get("a").put("" + i, i);
		// }
		// if (i < 40) {
		// vTable.get("h").get("b").put("" + i, i);
		// }
		// if (i < 20) {
		// vTable.get("e").get("c").put("" + i, i);
		// }
		// }
		// jOptimization.getLeftJoinOptimizedSequence(tables);
	}
}
