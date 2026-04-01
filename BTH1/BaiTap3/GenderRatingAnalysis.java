package com.baitap1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderRatingAnalysis {

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("M:" + parts[1].trim()));
            }
        }
    }

    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("U:" + parts[1].trim()));
            }
        }
    }

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                context.write(new Text(parts[0].trim()), new Text("R:" + parts[1].trim() + ":" + parts[2].trim()));
            }
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        private static Map<String, String> movieNames = new HashMap<>();
        private static Map<String, List<String>> genderRatings = new HashMap<>(); // MovieID -> List of "Gender:Rating"

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String gender = "";
            List<String> ratingsForThisUser = new ArrayList<>();

            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("M:")) {
                    movieNames.put(key.toString(), str.substring(2));
                } else if (str.startsWith("U:")) {
                    gender = str.substring(2);
                } else if (str.startsWith("R:")) {
                    ratingsForThisUser.add(str.substring(2)); // MovieID:Rating
                }
            }

            if (!gender.isEmpty() && !ratingsForThisUser.isEmpty()) {
                for (String r : ratingsForThisUser) {
                    String[] rParts = r.split(":");
                    String mId = rParts[0];
                    String score = rParts[1];
                    genderRatings.computeIfAbsent(mId, k -> new ArrayList<>()).add(gender + ":" + score);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, List<String>> entry : genderRatings.entrySet()) {
                String mId = entry.getKey();
                String mName = movieNames.getOrDefault(mId, "Movie_" + mId);
                
                double mSum = 0, fSum = 0;
                int mCount = 0, fCount = 0;

                for (String gr : entry.getValue()) {
                    String[] parts = gr.split(":");
                    double score = Double.parseDouble(parts[1]);
                    if (parts[0].equalsIgnoreCase("M")) {
                        mSum += score; mCount++;
                    } else {
                        fSum += score; fCount++;
                    }
                }

                String result = String.format("Male_Avg: %s, Female_Avg: %s", 
                    (mCount > 0 ? String.format("%.2f", mSum/mCount) : "N/A"),
                    (fCount > 0 ? String.format("%.2f", fSum/fCount) : "N/A"));
                
                context.write(new Text(mName + ":"), new Text(result));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating Analysis");
        job.setJarByClass(GenderRatingAnalysis.class);
        job.setReducerClass(GenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}