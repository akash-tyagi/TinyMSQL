package database;

import storageManager.Disk;
import storageManager.MainMemory;
import storageManager.SchemaManager;

public class Manager {
	SchemaManager schema_manager;
	MainMemory mem;
	Disk disk;

	public Manager() {
		mem = new MainMemory();
		disk = new Disk();
		System.out.print("The memory contains " + mem.getMemorySize()
				+ " blocks" + "\n");
		System.out.print(mem + "\n" + "\n");
		schema_manager = new SchemaManager(mem, disk);
	}
}
