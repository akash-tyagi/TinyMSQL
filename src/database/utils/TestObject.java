package database.utils;

import java.util.ArrayList;
import java.util.Arrays;

public class TestObject {

    public static void main(String[] args) {
//        try {
//            Integer.parseInt("-1");
//            System.out.println("Haha");
//        } catch (Exception e) {
//            System.out.println("Nana");
//        }
        
            ArrayList<String> temp = OnePassUtils.getCommmonCols(new ArrayList<>(Arrays.asList("homework", "grade")), 
                                                                 new ArrayList<>(Arrays.asList("homework1", "grade")));
            System.out.println(temp);
    }
}
