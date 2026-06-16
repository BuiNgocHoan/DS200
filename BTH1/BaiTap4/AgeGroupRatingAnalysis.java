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

public class AgeGroupRatingAnalysis {

    // Mapper cho Movies: MovieID -> "M:MovieTitle"
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 2) {
                context.write(new Text(parts[0].trim()), new Text("M:" + parts[1].trim()));
            }
        }
    }

    // Mapper cho Users: UserID -> "U:AgeGroup"
    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                int age = Integer.parseInt(parts[2].trim());
                String ageGroup = "";
                if (age < 18) ageGroup = "0-18";
                else if (age <= 35) ageGroup = "18-35";
                else if (age <= 50) ageGroup = "35-50";
                else ageGroup = "50+";
                
                context.write(new Text(parts[0].trim()), new Text("U:" + ageGroup));
            }
        }
    }

    // Mapper cho Ratings: UserID -> "R:MovieID:Rating"
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 3) {
                context.write(new Text(parts[0].trim()), new Text("R:" + parts[1].trim() + ":" + parts[2].trim()));
            }
        }
    }

    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {
        private static Map<String, String> movieNames = new HashMap<>();
        private static Map<String, Map<String, List<Double>>> ageGroupRatings = new HashMap<>(); // MovieID -> (AgeGroup -> List of Ratings)

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ageGroup = "";
            List<String> ratingsForUser = new ArrayList<>();

            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("M:")) {
                    movieNames.put(key.toString(), str.substring(2));
                } else if (str.startsWith("U:")) {
                    ageGroup = str.substring(2);
                } else if (str.startsWith("R:")) {
                    ratingsForUser.add(str.substring(2));
                }
            }

            if (!ageGroup.isEmpty() && !ratingsForUser.isEmpty()) {
                for (String r : ratingsForUser) {
                    String[] rParts = r.split(":");
                    String mId = rParts[0];
                    double score = Double.parseDouble(rParts[1]);
                    
                    ageGroupRatings.computeIfAbsent(mId, k -> new HashMap<>())
                                  .computeIfAbsent(ageGroup, k -> new ArrayList<>())
                                  .add(score);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String mId : ageGroupRatings.keySet()) {
                String mName = movieNames.getOrDefault(mId, "Movie_" + mId);
                Map<String, List<Double>> groups = ageGroupRatings.get(mId);
                
                StringBuilder sb = new StringBuilder("[");
                String[] categories = {"0-18", "18-35", "35-50", "50+"};
                
                for (String cat : categories) {
                    List<Double> scores = groups.get(cat);
                    String avg = "N/A";
                    if (scores != null && !scores.isEmpty()) {
                        double sum = 0;
                        for (double s : scores) sum += s;
                        avg = String.format("%.2f", sum / scores.size());
                    }
                    sb.append(cat).append(": ").append(avg).append(cat.equals("50+") ? "" : ", ");
                }
                sb.append("]");
                context.write(new Text(mName + ":"), new Text(sb.toString()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Rating Analysis");
        job.setJarByClass(AgeGroupRatingAnalysis.class);
        job.setReducerClass(AgeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}