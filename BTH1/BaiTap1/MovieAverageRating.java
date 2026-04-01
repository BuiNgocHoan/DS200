package com.baitap1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieAverageRating {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                context.write(new Text(parts[1].trim()), new Text("R," + parts[2].trim()));
            }
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("M," + parts[1].trim()));
            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private String maxMovie = "";
        private double maxRating = -1.0;

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown";
            double sumRating = 0;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("R")) {
                    sumRating += Double.parseDouble(parts[1]);
                    count++;
                } else if (parts[0].equals("M")) {
                    movieTitle = parts[1];
                }
            }

            if (count > 0) {
                double avg = sumRating / count;
                context.write(new Text(movieTitle), new Text("AverageRating: " + String.format("%.2f", avg) + " (TotalRatings: " + count + ")"));

                if (count >= 5 && avg > maxRating) {
                    maxRating = avg;
                    maxMovie = movieTitle;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovie.isEmpty()) {
                context.write(new Text("\nRESULT:"), new Text(maxMovie + " is the highest rated movie with an average rating of " + maxRating + " among movies with at least 5 ratings."));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Average Rating");
        job.setJarByClass(MovieAverageRating.class);
        job.setReducerClass(RatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);   // movies.txt
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);  // ratings_1.txt
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class);  // ratings_2.txt

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}