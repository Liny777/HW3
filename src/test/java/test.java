import java.io.*;

public class test {
    public static void main(String[] args) throws Exception {

        try {
            BufferedReader in = new BufferedReader(new FileReader("/Users/linyouguang/IdeaProjects/HW3/src/dataset/input/centroids.txt"));
            String str;
            while ((str = in.readLine()) != null) {
                String[] temp = str.split(" ");

                System.out.println(temp[2].substring(1));
                System.out.println(" ");
//                System.out.println(str);
            }
//            System.out.println(str);
        } catch (IOException e) {
        }
    }
}