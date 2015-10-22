package database;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.io.IOException;
import java.io.InputStreamReader;

import database.parser.Parser;

public class Interface {
	public ArrayList<String> queries;

	public Interface() {
		queries = new ArrayList<String>();
	}

	/* Read single query from console */
	public void readText() throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		System.out.print("Enter Query:");
		String q = br.readLine();
		queries.add(q);
	}

	public void parseQueries() {
		Parser parser = new Parser(queries.get(0));
		parser.parse();
	}

	public static void main(String[] args) throws IOException {
		Interface iface = new Interface();
		iface.readText();
		iface.parseQueries();
	}
}
