package database.utils;

import database.DbManager;
import java.util.ArrayList;
import storageManager.Relation;

public class TwoPassUtils {

    public static void sortRelationForPhase1(DbManager dbManager, String tableName, ArrayList<String> sortingCols) {
        int memSize = dbManager.mem.getMemorySize();
        Relation relation = dbManager.schema_manager.getRelation(tableName);

        int subListStart = 0;
        int subListEnd = (relation.getNumOfBlocks() - subListStart >= memSize) ? memSize : relation.getNumOfBlocks();

        while (subListStart < subListEnd) {
            relation.getBlocks(subListStart, 0, subListEnd - subListStart);
            GeneralUtils.sortMainMemory(dbManager, sortingCols, subListEnd - subListStart);
            relation.setBlocks(subListStart, 0, subListEnd - subListStart);
            subListStart = subListEnd;
            subListEnd = (relation.getNumOfBlocks() - subListEnd >= subListEnd + memSize) ? subListEnd + memSize : relation.getNumOfBlocks();
        }
    }

}
