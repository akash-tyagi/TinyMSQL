package database.utils;

import java.util.ArrayList;
import storageManager.Tuple;

public class TupleObject implements Comparable<TupleObject> {

    public Tuple tuple;
    ArrayList<String> colValues = new ArrayList<>();

    public TupleObject(Tuple tuple, ArrayList<String> colValues) {
        this.tuple = tuple;
        this.colValues = colValues;
    }

    @Override
    public int compareTo(TupleObject compareTuple) {
        for (int i = 0; i < this.colValues.size(); i++) {
            try {
                int a = Integer.parseInt(this.colValues.get(i));
                int b = Integer.parseInt(compareTuple.colValues.get(i));
                if (a != b) {
                    return a - b;
                }
            } catch (Exception e) {
                if (!this.colValues.get(i).equals(compareTuple.colValues.get(i))) {
                    return this.colValues.get(i).compareTo(compareTuple.colValues.get(i));
                }
            }
        }
        return 0;
    }
}
