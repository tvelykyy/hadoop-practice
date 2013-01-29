package com.tvelykyy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountExtended {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            /* Stripping line. Only chars, numbers and spaces would be left. */
            line = line.replaceAll("[^a-zA-Z0-9\\s]", "");
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                /* Omitting numbers and words less than 3 characters. */
                if (!token.matches("[0-9]+") && !token.matches(".{1,2}")) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            /* Omitting words which occur less than 5 times. */
            if (sum >= 5) {
                context.write(key, new IntWritable(sum));
            } else {
                context.getCounter(COUNTERS.OMITTED_WORDS).increment(1);
            }
        }
    }

    public static enum COUNTERS {
        OMITTED_WORDS
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcount");

        /* Needed for preudo-distributed usage. */
        job.setJarByClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        Counters counters = job.getCounters();
        Counter omittedWordsCounter = counters.findCounter(COUNTERS.OMITTED_WORDS);
        System.out.println("Omitted words count: " + omittedWordsCounter.getValue());
    }

}
