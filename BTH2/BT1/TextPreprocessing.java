package com.baitap2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

public class TextPreprocessing {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                String line;
                while ((line = reader.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }
                reader.close();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            if (parts.length > 1) {
                String review = parts[1].toLowerCase();
                
                String[] words = review.split("[\\s\\p{Punct}]+");
                StringBuilder cleanedReview = new StringBuilder();

                for (String word : words) {
                    word = word.trim();
                    // Kiểm tra nếu từ KHÔNG nằm trong danh sách stopWords
                    if (!word.isEmpty() && !stopWords.contains(word)) {
                        cleanedReview.append(word).append(" ");
                    }
                }
                
                if (cleanedReview.length() > 0) {
                    context.write(new Text(parts[0]), new Text(cleanedReview.toString().trim()));
                }
            }
        }
    }

    public static class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Text Preprocessing");
        job.setJarByClass(TextPreprocessing.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(PreprocessingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[1]).toUri()); 

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}