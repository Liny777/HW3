package HW3_B;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable {

    private float[] components = null;
    private int dim;
    private int numPoints; // For partial sums

    public Point() {
        this.dim = 0;
    }

    public Point(final float[] c) {
        this.set(c);
    }

    public Point(final String[] s) {
        this.set(s);
    }

    public static Point copy(final Point p) {
        Point ret = new Point(p.components);
        ret.numPoints = p.numPoints;
        return ret;
    }

    public void set(final float[] c) {
        this.components = c;
        this.dim = c.length;
        this.numPoints = 1;
    }

    public void set(final String[] s) {
        this.components = new float[s.length];
        this.dim = s.length;
        this.numPoints = 1;
        for (int i = 0; i < s.length; i++) {
            this.components[i] = Float.parseFloat(s[i]);
        }
    }

    public void setNumPoints(int numPoints) {
        this.numPoints = numPoints;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.numPoints = in.readInt();
        this.components = new float[this.dim];

        for(int i = 0; i < this.dim; i++) {
            this.components[i] = in.readFloat();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.numPoints);

        for(int i = 0; i < this.dim; i++) {
            out.writeFloat(this.components[i]);
        }
    }

    public int getNumPoints() {
        return numPoints;
    }

    @Override
    public String toString() {
        StringBuilder point = new StringBuilder();
        for (int i = 0; i < this.dim; i++) {
            point.append(Float.toString(this.components[i]));
            if(i != dim - 1) {
                point.append(",");
            }
        }
        return point.toString();
    }

    public void sum(Point p) {
        for (int i = 0; i < this.dim; i++) {
            this.components[i] += p.components[i];
        }
        this.numPoints += p.numPoints;
    }

    public float distance(Point p){

        // Euclidean
        float dist = 0.0f;
        for (int i = 0; i < this.dim; i++) {
            // 每个维度进行做差
            dist += Math.pow(Math.abs(this.components[i] - p.components[i]), 2);
        }
        // 欧几里得距离，每个元素之间做差平方相加后对其结果开根号
        dist = (float)Math.round(Math.pow(dist, 1f/2)*100000)/100000.0f;
        return dist;
    }

    public void average() {
        for (int i = 0; i < this.dim; i++) {
            float temp = this.components[i] / this.numPoints;
            this.components[i] = (float)Math.round(temp*100000)/100000.0f;
        }
//        this.numPoints = 1;
    }
}