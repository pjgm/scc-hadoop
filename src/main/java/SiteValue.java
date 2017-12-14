import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SiteValue implements WritableComparable<SiteValue> {

    private IntWritable urlCount;
    private LongWritable avgSize;

    public SiteValue() {
        this.urlCount = new IntWritable();
        this.avgSize = new LongWritable();
    }

    public SiteValue(IntWritable urlCount, LongWritable avgSize) {
        this.urlCount = urlCount;
        this.avgSize = avgSize;
    }

    public void write(DataOutput dataOutput) throws IOException {
        urlCount.write(dataOutput);
        avgSize.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        urlCount.readFields(dataInput);
        avgSize.readFields(dataInput);
    }

    public int compareTo(SiteValue siteValue) {
        return siteValue.urlCount.compareTo(this.urlCount);
    }

    public IntWritable getUrlCount() {
        return urlCount;
    }

    public LongWritable getAvgSize() {
        return avgSize;
    }

    @Override
    public String toString() {
        return getUrlCount() + "\t" + getAvgSize();
    }
}
