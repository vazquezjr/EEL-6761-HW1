/*
    Name: Ruben Vazquez
    UFID: 2143-1351
    Title: WordCount.java part 3
    Description: Count the occurrences of all words that appear in the given inputs that match the patterns in the word-patterns.txt file
*/

package HW1;

import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;

public class WordCount extends Configured implements Tool {

    // Mapper that only looks for certain words in a given set of patterns before mapping an occurrence of 1.
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private Set<String> word_patterns = new HashSet<String>();

        // Obtain the word patterns from the distributed cache.
	public void configure(JobConf job) {

            Path pattern_file;
            Scanner pattern_in;

            try {
                pattern_file = DistributedCache.getLocalCacheFiles(job)[0];
                pattern_in = new Scanner(new FileReader(pattern_file.toString()));
            }
            catch (IOException e) {
                System.out.println("Couldn't get cache file");
                return;
            }

            while (pattern_in.hasNext()) {
                word_patterns.add(pattern_in.next());
            }

	}

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            String temp = null;

            while (tokenizer.hasMoreTokens()) {

		temp = new String(tokenizer.nextToken());

                if (word_patterns.contains(temp)) {

                    word.set(temp);
                    output.collect(word, one);

                }
            }
        }
    }

    // Reducer that sums the occurence of the words from the given word patterns.
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext())
                sum += values.next().get();
            output.collect(key, new IntWritable(sum));
        }
    }

    // Set up the job configuration, add the word patterns to the distributed cache, and run the job configuration.
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat .setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        DistributedCache.addCacheFile(new Path(args[2]).toUri(), conf);

        JobClient.runJob(conf);
        return 0;
    }

    // Run the job configuration.
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

}
