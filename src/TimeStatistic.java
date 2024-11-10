import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TestNoJobSetupCleanup.MyOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TimeStatistic {

    private static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text timeHour = new Text();
        private Text timeDay = new Text();
        private Text timeWeekDay = new Text();
        private Text timeMonth = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringReader stringReader = new StringReader(value.toString());
            BufferedReader bufferedReader = new BufferedReader(stringReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                Event event = new Event(line);
                if (event.isParsed()) {
                    final Pattern pattern = Pattern.compile("\\[([A-Za-z]+) ([A-Za-z]+) (\\d{2}) (\\d{2}):\\d{2}:\\d{2} \\d{4}\\]");
                    final Matcher matcher = pattern.matcher(line);

                    if (matcher.find()) {
                        timeWeekDay.set("Week Day: " + matcher.group(1));
                        timeMonth.set("Month: " + matcher.group(2));
                        timeDay.set("Day: " + matcher.group(3));
                        timeHour.set("Hour: " + matcher.group(4));
                        context.write(timeWeekDay, one);
                        context.write(timeMonth, one);
                        context.write(timeDay, one);
                        context.write(timeHour, one);
                    }
                }
            }
        }
    }

    private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class MyCombiner extends MyReducer {
    }

    public static Job getJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = new Job(conf, "time statistic");
        job.setJarByClass(ErrorStatistic.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(MyOutputFormat.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: TimeStatistic <in> <out>");
            System.exit(2);
        }
        Job job = getJob(conf, new Path(otherArgs[0]), new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
