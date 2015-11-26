package database;

import java.io.BufferedReader;
import java.util.ArrayList;
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
			if (stmt instanceof SelectStmt) {
				LogicalQuery logicalQuery = new LogicalQuery((SelectStmt) stmt);
			}
		}
	}

	public static void main(String[] args) throws IOException {
		// Interface iface = new Interface();
		// iface.readFile("src/testQueries");
		// iface.parseQueries();
		JoinOptimization jOptimization = new JoinOptimization();
		List<String> tables = new ArrayList<>();
		tables.add("a");
		tables.add("b");
		tables.add("c");
		tables.add("d");
		jOptimization.getLeftJoinOptimizedSequence(tables);
	}
}
