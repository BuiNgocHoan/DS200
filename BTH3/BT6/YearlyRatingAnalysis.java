package com.baitap3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public class YearlyRatingAnalysis {

    private static int getYearFromTimestamp(long timestamp) {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), ZoneId.of("UTC")).getYear();
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Yearly Rating Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        String basePath = "C:/Users/Administrator/Downloads/ds200_lab3/";

        JavaRDD<String> ratingsLines = sc.textFile(basePath + "ratings_1.txt," + basePath + "ratings_2.txt");
        String firstRatingLine = ratingsLines.first();
        
        JavaPairRDD<Integer, Tuple2<Double, Integer>> yearRatingPairs = ratingsLines
                .filter(line -> !line.equals(firstRatingLine))
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    double rating = Double.parseDouble(parts[2].trim());
                    long timestamp = Long.parseLong(parts[3].trim());
                    
                    int year = getYearFromTimestamp(timestamp);
                    
                    return new Tuple2<>(year, new Tuple2<>(rating, 1));
                });

        JavaPairRDD<Integer, Tuple2<Double, Integer>> reducedYearly = yearRatingPairs.reduceByKey((v1, v2) ->
                new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2())
        );

        JavaPairRDD<Integer, Tuple2<Double, Integer>> finalStats = reducedYearly.mapValues(v ->
                new Tuple2<>(v._1() / v._2(), v._2())
        );

        JavaPairRDD<Integer, Tuple2<Double, Integer>> sortedStats = finalStats.sortByKey(true);

        System.out.println(String.format("%-10s | %-10s | %s", "Nam", "Diem TB", "Tong Luot Danh Gia"));

        List<Tuple2<Integer, Tuple2<Double, Integer>>> results = sortedStats.collect();
        for (Tuple2<Integer, Tuple2<Double, Integer>> result : results) {
            int year = result._1();
            double avgRating = result._2()._1();
            int count = result._2()._2();
            System.out.println(String.format("%-10d | %-10.2f | %d", year, avgRating, count));
        }

        sc.stop();
    }
}