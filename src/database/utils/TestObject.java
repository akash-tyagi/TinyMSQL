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
        
//            ArrayList<String> temp = GeneralUtils.getCommmonCols(new ArrayList<>(Arrays.asList("homework", "grade")), 
//                                                                 new ArrayList<>(Arrays.asList("homework1", "grade")));
//            System.out.println(temp);
        
        String s1 = "abc";
        String s2 = "a" + "bc";
        
        if(s1 == s2)
            System.out.println("Hello");
        else
            System.out.println("Not Hello");
    }
}
