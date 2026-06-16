package com.baitap3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class AgeGroupRatingAnalysis {

    private static String getAgeDescription(int age) {
        if (age < 18) return "Duoi 18";
        if (age <= 24) return "18-24";
        if (age <= 34) return "25-34";
        if (age <= 44) return "35-44";
        if (age <= 49) return "45-49";
        if (age <= 55) return "50-55";
        return "Tren 56";
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Age Group Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        String basePath = "C:/Users/Administrator/Downloads/ds200_lab3/";

        JavaRDD<String> moviesLines = sc.textFile(basePath + "movies.txt");
        String firstMovieLine = moviesLines.first();
        Map<String, String> movieMap = moviesLines.filter(line -> !line.equals(firstMovieLine))
                .mapToPair(line -> {
                    String[] parts = line.split(",", 3);
                    return new Tuple2<>(parts[0].trim(), parts.length > 1 ? parts[1].trim() : "Unknown");
                }).collectAsMap();
        Broadcast<Map<String, String>> broadcastMovies = sc.broadcast(movieMap);

        JavaRDD<String> usersLines = sc.textFile(basePath + "users.txt");
        String firstUserLine = usersLines.first();
        
        JavaPairRDD<String, String> userAgeGroupsRDD = usersLines.filter(line -> !line.equals(firstUserLine))
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    String userId = parts[0].trim();
                    int age = Integer.parseInt(parts[2].trim());
                    return new Tuple2<>(userId, getAgeDescription(age));
                });

        JavaRDD<String> ratingsLines = sc.textFile(basePath + "ratings_1.txt," + basePath + "ratings_2.txt");
        String firstRatingLine = ratingsLines.first();

        JavaPairRDD<String, Tuple2<String, Double>> ratingsRDD = ratingsLines.filter(line -> !line.equals(firstRatingLine))
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    return new Tuple2<>(parts[0].trim(), new Tuple2<>(parts[1].trim(), Double.parseDouble(parts[2].trim())));
                });

        JavaPairRDD<String, Tuple2<Tuple2<String, Double>, String>> joinedRDD = ratingsRDD.join(userAgeGroupsRDD);

        JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Integer>> movieAgePairs = joinedRDD.mapToPair(tuple -> {
            String movieId = tuple._2()._1()._1();
            Double rating = tuple._2()._1()._2();
            String ageGroup = tuple._2()._2();
            return new Tuple2<>(new Tuple2<>(movieId, ageGroup), new Tuple2<>(rating, 1));
        });

        JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Integer>> reduced = movieAgePairs.reduceByKey((v1, v2) ->
                new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2())
        );

        JavaPairRDD<Tuple2<String, String>, Double> avgRatings = reduced.mapValues(v -> v._1() / v._2());

        System.out.println(String.format("%-40s | %-12s | %s", "Ten Phim", "Nhom Tuoi", "Diem TB"));

        avgRatings.take(30).forEach(result -> {
            String movieId = result._1()._1();
            String ageGroup = result._1()._2();
            Double avgScore = result._2();
            String movieTitle = broadcastMovies.getValue().getOrDefault(movieId, "Unknown");
            
            if (movieTitle.length() > 38) movieTitle = movieTitle.substring(0, 35) + "...";
            
            System.out.println(String.format("%-40s | %-12s | %.2f", movieTitle, ageGroup, avgScore));
        });

        sc.stop();
    }
}