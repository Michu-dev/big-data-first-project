import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

// Count class to implement counting in a proper way with combiner
public class Count implements WritableComparable<Count> {

    IntWritable count;

    public Count() {
        set(new IntWritable(0));
    }

    public Count(Integer count) {
        set(new IntWritable(count));
    }

    public void set(IntWritable count) {
        this.count = count;
    }

    public IntWritable getCount() {
        return count;
    }

    public void addCount(Count counter) {
        set(new IntWritable(this.count.get() + counter.getCount().get()));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        count.readFields(dataInput);
    }

    @Override
    public int compareTo(Count counter) {

        // return the value of compareTo between the "count" value
        return count.compareTo(counter.count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Count counter = (Count) o;

        return count.equals(counter.count);
    }

    @Override
    public int hashCode() {
        int result = count.hashCode();
        return result;
    }
}