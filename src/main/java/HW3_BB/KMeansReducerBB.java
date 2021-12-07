package HW3_BB;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducerBB extends Reducer<IntWritable, PointBB, Text, Text> {
    private final Text centroidId = new Text();
    private final Text centroidValue = new Text();

    public void reduce(IntWritable centroid, Iterable<PointBB> partialSums, Context context)
            throws IOException, InterruptedException {

        //Sum the partial sums
        PointBB sum = PointBB.copy(partialSums.iterator().next());
        while (partialSums.iterator().hasNext()) {
            sum.sum(partialSums.iterator().next());
        }
        //Calculate the new centroid
        sum.average();
        centroidId.set(centroid.toString()+","+sum.getNumPoints()+","+sum.getPercision()+","+sum.getMaxLabel()+","+sum.getClassLabel());
        String cenV = sum.toString();
        centroidValue.set(sum.getClassLabel()+","+cenV);
        context.write(centroidId, centroidValue);
    }


}
