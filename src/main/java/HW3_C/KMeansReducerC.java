package HW3_C;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducerC extends Reducer<IntWritable, PointC, Text, Text> {
    private final Text centroidId = new Text();
    private final Text centroidValue = new Text();

    public void reduce(IntWritable centroid, Iterable<PointC> partialSums, Context context)
            throws IOException, InterruptedException {

        //Sum the partial sums
        PointC sum = PointC.copy(partialSums.iterator().next());
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
