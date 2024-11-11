import java.io.IOException;
import java.util.StringTokenizer;
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
 * A Hadoop MapReduce to count the number of each word in logs.
 */
public class WordCount {

    /**
     * <p>
     * <code>MyMapper</code> is a <code>Mapper</code> class that extends the Hadoop
     * Mapper class. It processes input key-value pairs to generate a set of
     * intermediate key-value pairs.
     * </p>
     * 
     * <p>
     * The <code>map</code> method takes an input key-value pair and tokenizes the
     * value (which is a line of text). For each token (word) in the line, it writes
     * the word and a count of one to the context.
     * </p>
     * 
     * <p>
     * Key: <code>Object</code> (not used in this implementation) Value:
     * <code>Text</code> (a line of text from the input)
     * </p>
     * 
     * <p>
     * Output Key: <code>Text</code> (a word from the input line) Output Value:
     * <code>IntWritable</code> (the count of the word, which is always 1 in this
     * case)
     * </p>
     */
    private static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                token = token.replaceAll("^[\\pP]+|[\\pP]+$", "").toLowerCase(); // remove punctuations, to lowercase
                if (token.isEmpty() || Character.isDigit(token.charAt(0))) // ignore empty strings and numbers
                    continue;
                word.set(token);
                context.write(word, one);
            }
        }
    }

    /**
     * <p>
     * A Reducer class that extends the Hadoop Reducer class. It processes a set of
     * intermediate key-value pairs to generate a set of output key-value pairs.
     * </p>
     * 
     * <p>
     * The <code>reduce</code> method sums up all the IntWritable values associated
     * with a given key and writes the key and the sum to the context.
     * </p>
     * 
     * <p>
     * Key: Text (a word from the input line) Value: IntWritable (the count of the
     * word, which is always 1 in this case)
     * </p>
     * 
     * <p>
     * Output Key: Text (a word from the input line) Output Value: IntWritable (the
     * total count of the word)
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
     * Configures and returns a new Hadoop Job for counting words.
     *
     * @param conf   the Hadoop configuration to use for the job
     * @param input  the input path for the job
     * @param output the output path for the job
     * @return a configured Job instance for word counting
     */
    public static Job getJob(Configuration conf, Path input, Path output) throws IOException {
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount.class);
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
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = getJob(conf, new Path(otherArgs[0]), new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}