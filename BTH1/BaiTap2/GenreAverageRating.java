package com.baitap1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenreAverageRating {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                context.write(new Text(parts[1].trim()), new Text("R," + parts[2].trim()));
            }
        }
    }

    public static class GenreMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                context.write(new Text(parts[0].trim()), new Text("G," + parts[2].trim()));
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {
        private java.util.Map<String, Double> genreSum = new java.util.HashMap<>();
        private java.util.Map<String, Integer> genreCount = new java.util.HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Double> ratings = new ArrayList<>();
            String genresLine = "";

            for (Text val : values) {
                String[] parts = val.toString().split(",", 2);
                if (parts[0].equals("R")) {
                    ratings.add(Double.parseDouble(parts[1]));
                } else if (parts[0].equals("G")) {
                    genresLine = parts[1];
                }
            }

            if (!genresLine.isEmpty() && !ratings.isEmpty()) {
                String[] genres = genresLine.split("\\|");
                for (String g : genres) {
                    for (Double r : ratings) {
                        genreSum.put(g, genreSum.getOrDefault(g, 0.0) + r);
                        genreCount.put(g, genreCount.getOrDefault(g, 0) + 1);
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String g : genreSum.keySet()) {
                double avg = genreSum.get(g) / genreCount.get(g);
                context.write(new Text(g + ":"), new Text(String.format("%.2f", avg) + " (TotalRatings: " + genreCount.get(g) + ")"));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Average Rating");
        job.setJarByClass(GenreAverageRating.class);
        job.setReducerClass(GenreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GenreMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}