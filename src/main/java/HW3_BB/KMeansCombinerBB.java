package HW3_BB;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombinerBB extends Reducer<IntWritable, PointBB, IntWritable, PointBB> {
    public void reduce(IntWritable centroid, Iterable<PointBB> points, Context context)
            throws IOException, InterruptedException {

        //Sum the points
        PointBB sum = PointBB.copy(points.iterator().next());
        while (points.iterator().hasNext()) {
            sum.sum(points.iterator().next());
        }
        context.write(centroid, sum);
    }
}
