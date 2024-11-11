import java.io.IOException;

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
 * A Hadoop MapReduce to count the number of each type of errors in logs.
 */
public class ErrorStatistic {

    /**
     * <p>
     * MyMapper is a static inner class that extends the Mapper class. It processes
     * input key-value pairs to generate a set of intermediate key-value pairs.
     * </p>
     * 
     * <p>
     * The <code>map</code> method processes each line of the input, parses it into
     * an Event object, and checks if the event is parsed successfully. If the event
     * level is "<code>error</code>", it writes the event ID as the key and the
     * value 1 to the context. If the event is not parsed successfully, it writes
     * "<code>OTHER</code>" as the key.
     * </p>
     * 
     * <p>
     * Key: Object (input key, not used in this implementation) Value: Text (a line
     * of text from the input)
     * </p>
     * 
     * <p>
     * Output Key: Text (the event ID or "<code>OTHER</code>") Output Value:
     * IntWritable (the count of the event, which is always 1 in this case)
     * </p>
     */
    private static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text errorType = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Event event = new Event(line);
            if (event.isParsed()) {
                if (event.getLevel().equals("error")) {
                    errorType.set(event.gerEventId().toString());
                    context.write(errorType, one);
                }
            } else {
                errorType.set("OTHER");
                context.write(errorType, one);
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
     * The <code>reduce</code> method sums up the values for each key and writes the
     * key and the sum to the context.
     * </p>
     * 
     * <p>
     * Input Key: Text (the event ID or "OTHER") Input Value: IntWritable (the count
     * of the event)
     * </p>
     * 
     * <p>
     * Output Key: Text (the event ID or "OTHER") Output Value: IntWritable (the
     * total count of the event)
     * </p>
     */
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
     * Configures and returns a new Hadoop Job for ErrorStatistic.
     *
     * @param conf   the Hadoop configuration to use for the job
     * @param input  the input path for the job
     * @param output the output path for the job
     * @return a configured Job instance for error statistic
     */
    public static Job getJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = new Job(conf, "error statistic");
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
            System.err.println("Usage: ErrorStatistic <in> <out>");
            System.exit(2);
        }
        Job job = getJob(conf, new Path(otherArgs[0]), new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
