
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A Hadoop MapReduce to analyze Apache logs.
 */
public class ApacheLogAnalysis {
    public static void main(String[] args) throws Exception {
        // Parse paths
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ApacheLogAnalysis <in> <out>");
            System.exit(2);
        }
        Path input = new Path(otherArgs[0]);
        Path output = new Path(otherArgs[1]);
        
        // Delete output path if exists
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            System.out.println("Output path already exists. Press Enter to delete it or Ctrl+C to cancel.");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            reader.readLine(); // Wait for Enter
            fs.delete(output, true);
        }

        // Run jobs
        Job job1 = WordCount.getJob(conf, input, Path.mergePaths(output, new Path("/WordCount")));
        Job job2 = TimeStatistic.getJob(conf, input, Path.mergePaths(output, new Path("/TimeStatistic")));
        Job job3 = ErrorStatistic.getJob(conf, input, Path.mergePaths(output, new Path("/ErrorStatistic")));
        Job job4 = LogStructuring.getJob(conf, input, Path.mergePaths(output, new Path("/LogStructuring")));
        job1.submit();
        job2.submit();
        job3.submit();
        job4.submit();
        System.exit(job1.waitForCompletion(true) && job2.waitForCompletion(true) && job3.waitForCompletion(true)
                && job4.waitForCompletion(true) ? 0 : 1);
    }
}
