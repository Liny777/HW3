package HW3_B;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

    private Point[] centroids;
    private final Point point = new Point(); // 数据集里的每一个数据点
    private final IntWritable centroid = new IntWritable();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int k = Integer.parseInt(context.getConfiguration().get("typeOfCluster"));
        this.centroids = new Point[k];
        for(int i = 0; i < k; i++) {
            String[] centroid = context.getConfiguration().getStrings("centroid." + i);
            this.centroids[i] = new Point(centroid);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Contruct the point
        String[] pointString = value.toString().split(",");
        point.set(pointString);

        // Initialize variables
        float minDist = Float.POSITIVE_INFINITY;
        float distance = 0.0f;
        int nearest = -1;

        // Find the closest centroid
        for (int i = 0; i < centroids.length; i++) {
            distance = point.distance(centroids[i]);
            if(distance < minDist) {
                nearest = i;
                minDist = distance;
            }
        }

        centroid.set(nearest);
        context.write(centroid, point);

    }

}
