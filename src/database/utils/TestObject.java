package database.utils;

public class TestObject {

    public static void main(String[] args) {
        try {
            Integer.parseInt("-1");
            System.out.println("Haha");
        } catch (Exception e) {
            System.out.println("Nana");
        }
    }
}
