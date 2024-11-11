import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A Hadoop MapReduce to structure logs in CSV format.
 */
public class LogStructuring {

    /**
     * <p>
     * MyMapper is a static inner class that extends the Mapper class. It processes
     * input key-value pairs to generate a set of intermediate key-value pairs.
     * </p>
     * 
     * <p>
     * The <code>setup</code> method writes the header of the CSV file to the
     * context. The <code>map</code> method processes each line of the input, parses
     * it into an Event object, and writes the event in CSV format to the context.
     * </p>
     * 
     * <p>
     * Key: Object (input key, not used in this implementation) Value: Text (a line
     * of text from the input)
     * </p>
     * 
     * <p>
     * Output Key: IntWritable (a unique identifier for the event) Output Value:
     * Text (the event in CSV format)
     * </p>
     */
    private static class MyMapper extends Mapper<Object, Text, IntWritable, Text> {
        private static int count = 0;
        private static boolean first = true;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            if (first) {
                first = false;
                context.write(new IntWritable(0), new Text("Time,Level,Content,EventId,EventTemplate"));
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            Event event = new Event(line);
            context.write(new IntWritable(++count), new Text(event.toString()));
        }
    }

    /**
     * Configures and returns a new Hadoop Job for LogStructuring.
     *
     * @param conf   the Hadoop configuration to use for the job
     * @param input  the input path for the job
     * @param output the output path for the job
     * @return a configured Job instance for the LogStructuring job
     */
    public static Job getJob(Configuration conf, Path input, Path output) throws IOException {
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = new Job(conf, "csv structuring");
        job.setJarByClass(ErrorStatistic.class);
        job.setMapperClass(MyMapper.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: CsvStructuring <in> <out>");
            System.exit(2);
        }
        Job job = getJob(conf, new Path(otherArgs[0]), new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
