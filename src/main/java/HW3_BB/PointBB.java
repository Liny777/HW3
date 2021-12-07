package HW3_BB;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PointBB implements Writable {

    private float[] components = null;
    private int dim;
//    private int numPoints; // For partial sums
//    private int ownLabel; // 自身的label 由输入文件来
//    private int classLabel; // 这个数据点所属质心的label 由一开始生成的来
//    private int maxLabel; // 当前cluster里label数量最多的label
    private int numPoints; // For partial sums
    private int ownLabel; // 自身的label 由输入文件来
    private int classLabel; // 这个数据点所属质心的label 由一开始生成的来
    private int maxLabel; // 当前cluster里label数量最多的label
    private float percision;
    private int[] eachLabelNum = null;
    private int typeClass; // 有几个类
    public PointBB() {
        this.dim = 0;
    }

    public PointBB(final float[] c, int totalType, int centerClass, int pnumPoints, int pownLabel, final int[] e) {
        this.set(c,totalType,centerClass,pnumPoints,pownLabel,e);
    }

//    public PointBB(final String[] s, int totalType, int centerClass) {
//        this.set(s,totalType,centerClass);
//    }

    public PointBB(final String[] s, int totalType) {
        this.set(s,totalType);
    }

    public static PointBB copy(final PointBB p) {
        PointBB ret = new PointBB(p.components,p.typeClass,p.classLabel,p.numPoints,p.ownLabel,p.eachLabelNum);
        return ret;
    }

    public void set(final float[] c,int totalType,int centerClass,int pnumPoints,int pownLabel,int[] e) {
        this.components = c;
        this.dim = c.length;
        this.classLabel = centerClass;
        this.typeClass = totalType;
        this.numPoints = pnumPoints;
        this.ownLabel = pownLabel;
        this.eachLabelNum = e;
    }

    public void set(final String[] s,int totalType) {
        this.components = new float[s.length];
        this.dim = s.length;
        this.numPoints = 1;
        this.typeClass = totalType;
//        this.classLabel = centerClass;
        this.eachLabelNum = new int[totalType];
        this.ownLabel = (int) Float.parseFloat(s[0]);
//        this.ownLabel = Integer.parseInt(s[0]);
        for(int j=0;j<totalType;j++){
            if(j==this.ownLabel){
                this.eachLabelNum[j] = 1;
            }else{
                this.eachLabelNum[j] = 0;
            }
        }
        for (int i = 1; i < s.length; i++) {
            this.components[i-1] = Float.parseFloat(s[i]);
        }
    }

    public void setNumPoints(int numPoints) {
        this.numPoints = numPoints;
    }

    public float getPercision() {
        return percision;
    }
    public void setPercision(float percision) {
        this.percision = percision;
    }
    public int getNumPoints() {
        return numPoints;
    }

    public int getMaxLabel() {
        return maxLabel;
    }
    public void setMaxLabel(int maxLabel) {
        this.maxLabel = maxLabel;
    }

    public int getTypeClass() {
        return typeClass;
    }

    public void setTypeClass(int typeClass) {
        this.typeClass = typeClass;
    }

    public int getOwnLabel() {
        return ownLabel;
    }

    public void setOwnLabel(int ownLabel) {
        this.ownLabel = ownLabel;
    }

    public int getClassLabel() {
        return classLabel;
    }

    public void setClassLabel(int classLabel) {
        this.classLabel = classLabel;
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        this.dim = in.readInt();
        this.numPoints = in.readInt();
        this.ownLabel = in.readInt();
        this.classLabel = in.readInt();
        this.typeClass = in.readInt();
        this.maxLabel = in.readInt();
        this.components = new float[this.dim];
        this.eachLabelNum = new int[this.typeClass];
        for(int i = 0; i < this.dim; i++) {
            this.components[i] = in.readFloat();
        }
        for(int i = 0; i < this.typeClass; i++) {
            this.eachLabelNum[i] = in.readInt();
        }
    }

    @Override
    public void write(final DataOutput out) throws IOException {
        out.writeInt(this.dim);
        out.writeInt(this.numPoints);
        out.writeInt(this.ownLabel);
        out.writeInt(this.classLabel);
        out.writeInt(this.typeClass);
        out.writeInt(this.maxLabel);
        for(int i = 0; i < this.dim; i++) {
            out.writeFloat(this.components[i]);
        }
        for(int j = 0; j < this.typeClass; j++) {
            out.writeInt(this.eachLabelNum[j]);
        }
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

    public void sum(PointBB p) {
        for (int i = 0; i < this.dim; i++) {
            this.components[i] += p.components[i];
        }
        this.eachLabelNum[p.ownLabel] += p.eachLabelNum[p.ownLabel];
        this.numPoints += p.numPoints;
    }

    public float distance(PointBB p){
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

    public void average()
    {
        for (int i = 0; i < this.dim; i++) {
            float temp = this.components[i] / this.numPoints;
            this.components[i] = (float)Math.round(temp*100000)/100000.0f;
        }
        this.percision = this.eachLabelNum[this.classLabel];
        int max = 0;
        for(int j=0;j<this.typeClass;j++)
        {
            if(this.eachLabelNum[j]>max)
            {
                this.maxLabel = j;
                max = this.eachLabelNum[j];
            }
        }
    }
}