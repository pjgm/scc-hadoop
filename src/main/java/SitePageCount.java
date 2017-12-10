import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import warc.WARCFileInputFormat;

import java.io.IOException;
import java.net.URL;

public class SitePageCount {

    public static class SiteMapper extends Mapper<Text, ArchiveReader, Text, LongWritable> {

        public void map(Text key, ArchiveReader value, Context context) throws IOException, InterruptedException {

            for (ArchiveRecord record : value) {

                if(!record.getHeader().getMimetype().equals("text/plain"))
                    continue; // ignore other record types

                URL url = new URL(record.getHeader().getUrl());

                Text host = new Text();
                host.set(url.getHost());

                context.write(host, new LongWritable(1));
            }

        }
    }

    public static class ArchiveRecordReducer extends Reducer<Text, LongWritable , Text , LongWritable> {

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (LongWritable val: values) {
                System.out.println(val);
                sum += val.get();
            }

            LongWritable total = new LongWritable();
            total.set(sum);
            context.write(key, total);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Site Counter");
        job.setJarByClass(SitePageCount.class);
        job.setMapperClass(SitePageCount.SiteMapper.class);
        job.setCombinerClass(SitePageCount.ArchiveRecordReducer.class);
        job.setReducerClass(SitePageCount.ArchiveRecordReducer.class);
        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
