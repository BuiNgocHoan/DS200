package com.baitap2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CategoryRelatedWords {

    public static class RelatedWordMapper extends Mapper<Object, Text, Text, Text> {
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] cacheFiles = context.getLocalCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(
                        new FileInputStream(cacheFiles[0].toString()), StandardCharsets.UTF_8));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.trim().isEmpty()) stopWords.add(line.trim().toLowerCase());
                }
                reader.close();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(";");
            if (parts.length >= 4) {
                String category = parts[3].trim();
                String review = parts[1].toLowerCase();

                if (!category.isEmpty()) {
                    String[] words = review.split("[\\s\\p{Punct}]+");
                    for (String word : words) {
                        word = word.trim();
                        if (word.length() > 1 && !stopWords.contains(word)) {
                            context.write(new Text(category), new Text(word));
                        }
                    }
                }
            }
        }
    }

    public static class RelatedWordReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> wordCounts = new HashMap<>();
            for (Text val : values) {
                String word = val.toString();
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + 1);
            }

            List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCounts.entrySet());
            list.sort((a, b) -> b.getValue().compareTo(a.getValue()));

            StringBuilder result = new StringBuilder();
            int count = 0;
            for (Map.Entry<String, Integer> entry : list) {
                if (count >= 5) break;
                result.append(entry.getKey()).append("(").append(entry.getValue()).append(") ");
                count++;
            }
            context.write(key, new Text(result.toString().trim()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Category Related Words");
        job.setJarByClass(CategoryRelatedWords.class);
        job.setMapperClass(RelatedWordMapper.class);
        job.setReducerClass(RelatedWordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new Path(args[1]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}