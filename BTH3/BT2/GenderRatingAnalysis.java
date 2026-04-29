package com.baitap3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class GenderRatingAnalysis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Gender Rating Analysis").setMaster("local[*]");
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
        
        JavaPairRDD<String, String> usersRDD = usersLines.filter(line -> !line.equals(firstUserLine))
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    return new Tuple2<>(parts[0].trim(), parts[1].trim());
                });

        JavaRDD<String> ratingsLines = sc.textFile(basePath + "ratings_1.txt," + basePath + "ratings_2.txt");
        String firstRatingLine = ratingsLines.first();
        
        JavaPairRDD<String, Tuple2<String, Double>> ratingsRDD = ratingsLines.filter(line -> !line.equals(firstRatingLine))
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    String userId = parts[0].trim();
                    String movieId = parts[1].trim();
                    double rating = Double.parseDouble(parts[2].trim());
                    return new Tuple2<>(userId, new Tuple2<>(movieId, rating));
                });

        JavaPairRDD<String, Tuple2<Tuple2<String, Double>, String>> joinedRDD = ratingsRDD.join(usersRDD);

        JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Integer>> movieGenderPairs = joinedRDD.mapToPair(tuple -> {
            String movieId = tuple._2()._1()._1();
            Double rating = tuple._2()._1()._2();
            String gender = tuple._2()._2();

            return new Tuple2<>(new Tuple2<>(movieId, gender), new Tuple2<>(rating, 1));
        });

        JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Integer>> reduced = movieGenderPairs.reduceByKey((v1, v2) ->
                new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2())
        );

        JavaPairRDD<Tuple2<String, String>, Double> avgRatings = reduced.mapValues(v -> v._1() / v._2());

        System.out.println(String.format("%-40s | %-10s | %s", "Ten Phim", "Gioi Tinh", "Diem TB"));

        avgRatings.take(30).forEach(result -> {
            String movieId = result._1()._1();
            String rawGender = result._1()._2();
            String gender = rawGender.equals("M") ? "Nam (M)" : (rawGender.equals("F") ? "Nu (F)" : rawGender);
            Double avgScore = result._2();
            
            String movieTitle = broadcastMovies.getValue().getOrDefault(movieId, "Unknown ID");
            if (movieTitle.length() > 38) movieTitle = movieTitle.substring(0, 35) + "...";
            
            System.out.println(String.format("%-40s | %-10s | %.2f", movieTitle, gender, avgScore));
        });

        sc.stop();
    }
}