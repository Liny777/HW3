package HW3_B;//package HW3_B;
//
//import Homework3.hadoop.model.Point;
//import org.apache.hadoop.fs.FSDataInputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.stream.Stream;
//
//public class test {
//    public static void main(String[] args){
//        Point[] points = new Point[10];
////        Path path = new Path("/Users/linyouguang/IdeaProjects/A-priori/src/dataset/train-images.idx3-ubyte");
//        try (Stream<String> lines = Files.lines(Paths.get("/Users/linyouguang/IdeaProjects/A-priori/src/dataset/train/image/train_image.txt"))) {
//            String line32 = lines.skip(31).findFirst().get();
//            points[0] = new Point(line32.split(" "));
//            for(int i=0;i<points[0].getComponents().length;i++){
//                System.out.print(points[0].getComponents()[i]+" ");
//            }
////            System.out.println(line32);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
////        FileSystem hdfs = FileSystem.get(conf);
////        FSDataInputStream in = hdfs.open(path);
////        BufferedReader br = new BufferedReader(new InputStreamReader(in));
//    }
//}
