package database;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import database.parser.Parser;
import database.parser.StmtInterface;
import database.physicalquery.PhysicalTree;
import database.utils.GeneralUtils;

public class Interface {
	ArrayList<String> queries;
	DbManager dbManager;
	PrintWriter writer;

	public Interface()
			throws FileNotFoundException, UnsupportedEncodingException {
		queries = new ArrayList<>();
		dbManager = new DbManager();
		writer = new PrintWriter("OUTPUT.txt", "UTF-8");

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

	public void executeQueries() {
		Parser parser = new Parser(dbManager);
		for (String query : queries) {
			if (query.contains("#"))
				continue;
			System.out.println(query);
			writer.println(query);
			GeneralUtils.restartTimer(dbManager);

			StmtInterface stmt = parser.parse(query);
			PhysicalTree physicalTree = new PhysicalTree(query, dbManager, stmt,
					writer);
			physicalTree.execute();

			GeneralUtils.printExecutionStats(dbManager, writer);
			System.out.println("");
			writer.println();
		}
		writer.close();
	}

	public static void main(String[] args) throws IOException {
		Interface iface = new Interface();
		if (GlobalVariable.isReadFromConsole)
			iface.readText();
		else
			iface.readFile("INPUT.txt");
		iface.executeQueries();
	}
}
