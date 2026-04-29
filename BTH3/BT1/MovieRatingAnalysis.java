package com.baitap3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Map;

public class MovieRatingAnalysis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Movie Rating Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.setLogLevel("ERROR");

        String basePath = "C:/Users/Administrator/Downloads/ds200_lab3/";

        JavaRDD<String> moviesLines = sc.textFile(basePath + "movies.txt");
        
        String firstMovieLine = moviesLines.first();
        JavaRDD<String> filteredMovies = moviesLines.filter(line -> !line.equals(firstMovieLine));

        Map<String, String> movieMap = filteredMovies.mapToPair(line -> {
            String[] parts = line.split(",", 3);
            String id = parts[0].trim();
            String title = parts.length > 1 ? parts[1].trim() : "Unknown";
            return new Tuple2<>(id, title);
        }).collectAsMap();

        JavaRDD<String> ratingsLines = sc.textFile(basePath + "ratings_1.txt," + basePath + "ratings_2.txt");
        
        String firstRatingLine = ratingsLines.first();
        JavaRDD<String> filteredRatings = ratingsLines.filter(line -> !line.equals(firstRatingLine));

        JavaPairRDD<String, Tuple2<Double, Integer>> ratingPairs = filteredRatings.mapToPair(line -> {
            String[] parts = line.split(","); // Tách theo dấu phẩy
            String movieId = parts[1].trim();
            double rating = Double.parseDouble(parts[2].trim());
            return new Tuple2<>(movieId, new Tuple2<>(rating, 1));
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> reducedRatings = ratingPairs.reduceByKey((v1, v2) ->
                new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2())
        );

        JavaPairRDD<String, Tuple2<Double, Integer>> avgRatings = reducedRatings.mapValues(v ->
                new Tuple2<>(v._1() / v._2(), v._2())
        ).filter(t -> t._2()._2() >= 5);

        if (!avgRatings.isEmpty()) {
            Tuple2<String, Tuple2<Double, Integer>> bestMovie = avgRatings.reduce((t1, t2) ->
                    t1._2()._1() > t2._2()._1() ? t1 : t2
            );

            String movieId = bestMovie._1();
            double avgScore = bestMovie._2()._1();
            int count = bestMovie._2()._2();

            System.out.println("PHIM CO DIEM TRUNG BINH CAO NHAT (>= 50 luot danh gia):");
            System.out.println("ID Phim: " + movieId);
            System.out.println("Ten Phim: " + movieMap.getOrDefault(movieId, "Unknown Title"));
            System.out.println("Diem TB: " + String.format("%.2f", avgScore));
            System.out.println("Luot danh gia: " + count);
        } else {
            System.out.println("Khong co phim nao dat dieu kien (>= 50 luot danh gia).");
        }

        sc.stop();
    }
}