import java.io.IOException;
import java.util.regex.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A Hadoop MapReduce to count the number of events by time.
 */
public class TimeStatistic {

    /**
     * <p>
     * MyMapper is a static inner class that extends the Mapper class. It processes
     * input key-value pairs to generate a set of intermediate key-value pairs.
     * </p>
     * 
     * <p>
     * The <code>map</code> method processes each line of the input, parses it into
     * an Event object, and checks if the event is parsed successfully. If the event
     * is parsed successfully, it extracts the time information from the event and
     * writes the time information as the key and the value 1 to the context.
     * </p>
     * 
     * <p>
     * Key: Object (input key, not used in this implementation) Value: Text (a line
     * of text from the input)
     * </p>
     * 
     * <p>
     * Output Key: Text (the time information) Output Value: IntWritable (the count
     * of the time information, which is always 1 in this case)
     * </p>
     */
    private static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private static final Pattern pattern = Pattern
                .compile("\\[([A-Za-z]+) ([A-Za-z]+) (\\d{2}) (\\d{2}):\\d{2}:\\d{2} \\d{4}\\]");
        private Text timeHour = new Text();
        private Text timeDay = new Text();
        private Text timeWeekDay = new Text();
        private Text timeMonth = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Event event = new Event(line);
            if (event.isParsed()) {
                final Matcher matcher = pattern.matcher(event.getTime());
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

    /**
     * <p>
     * MyReducer is a static inner class that extends the Reducer class. It
     * processes a set of intermediate key-value pairs to generate a set of output
     * key-value pairs.
     * </p>
     * 
     * <p>
     * The reduce method sums up the values of the same key and writes the key and
     * the sum to the context.
     * </p>
     * 
     * <p>
     * Key: Text (the time information) Value: IntWritable (the count of the time
     * information)
     * </p>
     */
    private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
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

    /**
     * A combiner class that extends the {@link MyReducer} class.
     * 
     * It is an alias of {@link MyReducer}. This can help in reducing the amount of
     * data transferred across the network. Accelarates the process of reducing the
     * data.
     */
    private static class MyCombiner extends MyReducer {
    }

    /**
     * Configures and returns a new Hadoop Job for TimeStatistic.
     *
     * @param conf   the Hadoop configuration to use for the job
     * @param input  the input path for the job
     * @param output the output path for the job
     * @return a configured Job instance for time statistic
     */
    public static Job getJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = new Job(conf, "time statistic");
        job.setJarByClass(ErrorStatistic.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
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
