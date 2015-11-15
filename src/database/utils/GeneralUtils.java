package database.utils;

import storageManager.MainMemory;

public class GeneralUtils {
    public static void cleanMainMemory(MainMemory mem){
        for(int i = 0; i < mem.getMemorySize(); i++){
            mem.getBlock(i).invalidateTuples();
        }
    }
}
