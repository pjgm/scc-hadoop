import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.ArrayList;
import java.util.List;

public class TopWords {

    public static class SiteMapper extends Mapper<Text, ArchiveReader, Text, SiteValue> {

        private final static IntWritable ONE = new IntWritable(1);

        private List<String> getRecordWords(String content) {

            String[] words = content.replaceAll("\\p{P}", "").toLowerCase().split("\\s+");
            List<String> latinWords = new ArrayList<String>();

            for (String word : words)
                if (word.matches("\\p{IsLatin}+"))
                    latinWords.add(word);

            return latinWords;
        }

        public void map(Text key, ArchiveReader value, Context context) throws IOException, InterruptedException {

            for (ArchiveRecord record : value) {

                if(!record.getHeader().getMimetype().equals("text/plain"))
                    continue; // ignore other record types

                URL url = new URL(record.getHeader().getUrl());

                Text host = new Text();
                host.set(url.getHost());

                byte[] rawData = IOUtils.toByteArray(record, record.available());
                String content = new String(rawData);

                List<String> latinWords = getRecordWords(content);
                WordsCount wc = new WordsCount();

                for (String word: latinWords) {
                    wc.addWord(word);
                }

                wc.getTopWords();

//                System.out.println(isValid);
//                System.out.println(content.substring(0, Math.min(500, content.length())));
//                System.out.println((content.length() > 500 ? "..." : ""));
//
                // Pretty printing to make the output more readable
                System.out.println("=-=-=-=-=-=-=-=-=");
                SiteValue val = new SiteValue(ONE, new LongWritable(100));

                context.write(host, val);
            }

        }
    }

    public static class ArchiveRecordReducer extends Reducer<Text, SiteValue, Text , SiteValue> {

        public void reduce(Text key, Iterable<SiteValue> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            int count = 0;

            for (SiteValue val: values) {
                sum += val.getAvgSize().get();
                count += val.getUrlCount().get();
            }

            int avg = sum / count;

            IntWritable urlCount = new IntWritable(count);
            LongWritable avgSize = new LongWritable(avg);

            SiteValue result = new SiteValue(urlCount, avgSize);

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Site Counter");
        job.setJarByClass(TopWords.class);
        job.setMapperClass(TopWords.SiteMapper.class);
        job.setCombinerClass(TopWords.ArchiveRecordReducer.class);
        job.setReducerClass(TopWords.ArchiveRecordReducer.class);
        job.setInputFormatClass(WARCFileInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SiteValue.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
