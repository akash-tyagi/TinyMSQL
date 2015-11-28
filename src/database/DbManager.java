package database;

import java.util.HashMap;
import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

public class DbManager {
	public SchemaManager schema_manager;
	public MainMemory mem;
	public Disk disk;

	/*
	 * vTable has three levels of Mapping Level 1 : Map<relation_name, info1> --
	 * relation_name is table name (R) Level 2 (info1) : Map<attribute_name,
	 * info2> -- attribute_name is column name (A) Level 3 (info2) :
	 * Map<attribute_value, count> -- attribute_value is one of different
	 * attribute values -- for column A The V(R,A) value is size of Level 3 map.
	 * We are keeping a count to remove an attribute value from map when the
	 * count reaches 0 after repeated deletions.
	 */
	public HashMap<String, HashMap<String, HashMap<String, Integer>>> vTable;

	public DbManager() {
		mem = new MainMemory();
		disk = new Disk();
		System.out.print("The memory contains " + mem.getMemorySize()
				+ " blocks" + "\n");
		System.out.print(mem + "\n" + "\n");
		schema_manager = new SchemaManager(mem, disk);

		disk.resetDiskIOs();
		disk.resetDiskTimer();

		vTable = new HashMap<>();
	}
}
