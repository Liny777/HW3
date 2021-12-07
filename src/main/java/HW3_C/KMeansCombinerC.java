package HW3_C;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansCombinerC extends Reducer<IntWritable, PointC, IntWritable, PointC> {
    public void reduce(IntWritable centroid, Iterable<PointC> points, Context context)
            throws IOException, InterruptedException {

        //Sum the points
        PointC sum = PointC.copy(points.iterator().next());
        while (points.iterator().hasNext()) {
            sum.sum(points.iterator().next());
        }
        context.write(centroid, sum);
    }
}
