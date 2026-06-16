package com.baitap2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysis {

    public static class SentimentMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Cấu trúc: ID; Comment; Aspect; Category; Sentiment
            String[] parts = value.toString().split(";");
            if (parts.length >= 5) {
                String aspect = parts[2].trim();
                String sentiment = parts[4].trim().toLowerCase();
                
                if (!aspect.isEmpty() && (sentiment.equals("positive") || sentiment.equals("negative"))) {
                    context.write(new Text(aspect), new Text(sentiment));
                }
            }
        }
    }

    public static class SentimentReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int positiveCount = 0;
            int negativeCount = 0;

            for (Text val : values) {
                if (val.toString().equals("positive")) {
                    positiveCount++;
                } else if (val.toString().equals("negative")) {
                    negativeCount++;
                }
            }

            String result = String.format("Positive: %d, Negative: %d", positiveCount, negativeCount);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sentiment Analysis by Aspect");
        job.setJarByClass(SentimentAnalysis.class);
        job.setMapperClass(SentimentMapper.class);
        job.setReducerClass(SentimentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}