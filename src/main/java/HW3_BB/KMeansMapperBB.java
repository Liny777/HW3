package HW3_BB;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KMeansMapperBB extends Mapper<LongWritable, Text, IntWritable, PointBB> {

    private PointBB[] centroids;
    private final PointBB point = new PointBB(); // 数据集里的每一个数据点
    private final IntWritable centroid = new IntWritable();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int k = Integer.parseInt(context.getConfiguration().get("typeOfCluster"));
        this.centroids = new PointBB[k];
        for(int i = 0; i < k; i++) {
            String[] centroid = context.getConfiguration().getStrings("centroid." + i);
            this.centroids[i] = new PointBB(centroid,k);
            int cenLabel = Integer.parseInt(context.getConfiguration().get("centroid_Label." + i));
            this.centroids[i].setClassLabel(cenLabel);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Contruct the point
        String[] pointString = value.toString().split(",");
        int k = Integer.parseInt(context.getConfiguration().get("typeOfCluster"));
//        point.set(pointString,k);

        // Initialize variables
        float minDist = Float.POSITIVE_INFINITY;
        float distance = 0.0f;
        int nearest = -1;

        point.set(pointString,k);
        // Find the closest centroid
        for (int i = 0; i < centroids.length; i++) {
            distance = point.distance(centroids[i]);
            if(distance < minDist) {
                nearest = i;
                minDist = distance;
            }
        }
        centroid.set(nearest);
        point.setClassLabel(centroids[nearest].getClassLabel());
        context.write(centroid, point);

    }

//    @Override
//    protected void cleanup(Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
//    }

}
