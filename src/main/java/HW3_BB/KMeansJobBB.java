package HW3_BB;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class KMeansJobBB {

    // totalRow = 总行数 = 一共有多少样本点 60000
    // typeOfCluster = 定好要聚几个类别 10

    // centroidsInit方法用于初始化十个质心点
    private static PointBB[]  centroidsInit(Configuration conf, String pathString, int typeOfCluster, int totalRow) throws IOException {
        PointBB[] points = new PointBB[typeOfCluster];
        List<Integer> randomPosition = new ArrayList<Integer>();
        Random random = new Random();
        int postion_index; // 随机获取样本点的index
        // 循环，直到取出10个随机的位置坐标
        while(randomPosition.size() < typeOfCluster) {
            postion_index = random.nextInt(totalRow);
            if(!randomPosition.contains(postion_index)) {
                randomPosition.add(postion_index);
            }
        }
        // 对index进行排序
        Collections.sort(randomPosition);

        //File reading utils
        Path path = new Path(pathString);
        FileSystem hdfs = FileSystem.get(conf);
        FSDataInputStream in = hdfs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        int row = 0;
        int i = 0;
        int position;
        while(i < randomPosition.size()) {
            position = randomPosition.get(i); // 拿到刚刚随机分配的index
            String point = br.readLine();     // 读取每一行
            if(row == position) {
                String[] sss = point.split(",");
                points[i] = new PointBB(sss,typeOfCluster);
                points[i].setClassLabel(Integer.parseInt(sss[0]));
                i++;
            }
            row++;
        }
        br.close();

        return points;
    }
    // readCentroids函数用于读取上一个循环的输出的新的十个质心点
    private static PointBB[] readCentroids(Configuration conf, int typeOfCluster, String pathString ) throws IOException {
        PointBB[] points = new PointBB[typeOfCluster];
        FileSystem hdfs = FileSystem.get(conf);
        FileStatus[] status = hdfs.listStatus(new Path(pathString));
        for(int i=0;i<status.length;i++)
        {
            // 查看的就是output文件夹的路径
            // 匹配找到上一轮的输出文件，然后读取
            if(status[i].getPath().toString().endsWith("_SUCCESS")==false)
            {
                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(status[i].getPath())));
                String[] keyValueSplit = br.readLine().split("\t"); //Split line in K,V
                String[] id_num = keyValueSplit[0].split(",");
                int centroidId = Integer.parseInt(id_num[0]);
//              int centroidId = Integer.parseInt(keyValueSplit[0]);
                String[] point = keyValueSplit[1].split(","); // 每一维度使用逗号分割
                points[centroidId] = new PointBB(point,typeOfCluster);
//              points[centroidId].setClassLabel(Integer.parseInt(point[0]));
                points[centroidId].setNumPoints(Integer.parseInt(id_num[1]));
                points[centroidId].setPercision(Float.parseFloat(id_num[2]));
                points[centroidId].setMaxLabel(Integer.parseInt(id_num[3]));
                points[centroidId].setClassLabel(Integer.parseInt(id_num[4]));
                br.close();
            }
        }
        //Delete temp directory
        //读取完输出结果就把输出文件删除 这样就不会发生输出文件夹已存在的情况
        hdfs.delete(new Path(pathString), true);
        return points;
    }
    // 判断质心点是否收敛
    private static boolean whetherConvergence(PointBB[] oldCenter, PointBB[] newCenter, float threshold){
         for(int k=0;k<oldCenter.length;k++){
             if ( oldCenter[k].distance(newCenter[k]) <= threshold){

             }else{
                 return false;
             }
         }
         return true;
    }
    // 结果收敛时，进行的处理
    private static void storeResult(Configuration conf, PointBB[] finalPoints, String outputPath) throws IOException{
        FileSystem hdfs = FileSystem.get(conf);
        FSDataOutputStream dos = hdfs.create(new Path(outputPath + "/centroids.txt"), true);
        BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));
        // 将最后的结果写入文件里
        for(int i = 0; i < finalPoints.length; i++) {
            br.write("Centroid "+i+": ["+finalPoints[i].toString()+" ]"+", Number: "+finalPoints[i].getNumPoints()+", Correctly Nums: "+finalPoints[i].getPercision()+", Major Label: "+finalPoints[i].getMaxLabel()+",  Center Label: "+finalPoints[i].getClassLabel());
            br.newLine();
        }
        br.close();
        hdfs.close();
    }
    // 主函数
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // 设置初始的参数
        conf.setInt("typeOfCluster",10);
        conf.setInt("totalRow",60000);
        conf.setInt("Maxiteration",30);
        conf.setFloat("threshold",0.0001f);
//        final String INPUT = otherArgs[0];
//        final String OUTPUT = otherArgs[1] + "/temp";
        final String INPUT = "/user/s1155169171/homework3/data/combine/combine_train1.txt";
        final String OUTPUT = "/user/s1155169171/homework3/output";
//        final String INPUT = "/Users/linyouguang/IdeaProjects/HW3/src/dataset/combine/combine_train.txt";
//        final String OUTPUT = "/Users/linyouguang/IdeaProjects/HW3/src/output";
        final int DATASET_SIZE = conf.getInt("totalRow", 60000);
        final int K = conf.getInt("typeOfCluster", 10);
        final float THRESHOLD = conf.getFloat("threshold", 0.0001f);
        final int MAX_ITERATIONS = conf.getInt("Maxiteration", 30);
        PointBB[] oldCentroids = new PointBB[K];
        PointBB[] newCentroids = new PointBB[K];
        newCentroids = centroidsInit(conf, INPUT, K, DATASET_SIZE);
        // 通过conf.set给map传递每一次更新的质心
        for(int i = 0; i < K; i++) {
            conf.set("centroid." + i, newCentroids[i].toString());
            conf.set("centroid_Label." + i, Integer.toString(newCentroids[i].getClassLabel()));
        }

        // Map Reduce Process
        boolean stop_flag = false;
        boolean success_flag = true;
        int itera = 0;
        while(!stop_flag){
            itera ++ ;
            // Job Configuration
            Job job = Job.getInstance(conf, "iter_" + itera);
            job.setJarByClass(KMeansJobBB.class);
            job.setMapperClass(KMeansMapperBB.class);
            job.setCombinerClass(KMeansCombinerBB.class);
            job.setReducerClass(KMeansReducerBB.class);
            job.setNumReduceTasks(10); //one task each centroid
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(PointBB.class);
            FileInputFormat.addInputPath(job, new Path(INPUT));
            FileOutputFormat.setOutputPath(job, new Path(OUTPUT));
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // 如果哪个循环出错了，打印出哪个循环出错了并且推出应用程序
            success_flag = job.waitForCompletion(true);
            if(success_flag!=true){
                System.err.println("Iteration" + itera + "failed.");
                System.exit(1);
            }

            // 保存上一步的输出结果
            for(int id = 0; id<K; id ++){
                oldCentroids[id] = PointBB.copy(newCentroids[id]);
            }
            // 读取新的质心点
            newCentroids = readCentroids(conf,K,OUTPUT);
            // 检查新的质心与旧的质心是否相同，如果相同就退出循环
            stop_flag = whetherConvergence(oldCentroids,newCentroids,THRESHOLD);
            // 两个退出条件，一个是循环超过30次
            if( itera==(MAX_ITERATIONS) || stop_flag) {
                storeResult(conf,newCentroids,OUTPUT);
            }
            else {
                for(int x=0;x<K;x++)
                {
                    conf.unset("centroid."+x);
                    conf.set("centroid."+x,newCentroids[x].toString());
                }
            }
        }
        System.exit(0);
    }
}
