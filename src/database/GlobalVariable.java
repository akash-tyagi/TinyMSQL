package database;

public class GlobalVariable {
	public static boolean isTestExecution = false;
	public static boolean isTestParsing = false;
	public static boolean isTestJoinOpt = false;
	public static boolean isReadFromConsole = false;
	public static int USABLE_DATA_BLOCKS = 9;
	public static int TOTAL_DATA_BLOCKS = 10;
	// one left for other table and another for result storage
	public static int USABLE_JOIN_BLOCKS = 8;
}
