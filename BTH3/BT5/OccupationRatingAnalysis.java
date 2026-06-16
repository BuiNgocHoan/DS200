package com.baitap3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;

public class OccupationRatingAnalysis {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Occupation Rating Analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        String basePath = "C:/Users/Administrator/Downloads/ds200_lab3/";

        JavaRDD<String> occLines = sc.textFile(basePath + "occupation.txt");
        Map<String, String> occMap = occLines.mapToPair(line -> {
            String[] parts = line.split(","); // Giả định format: ID,Name
            return new Tuple2<>(parts[0].trim(), parts[1].trim());
        }).collectAsMap();
        Broadcast<Map<String, String>> broadcastOccNames = sc.broadcast(occMap);

        JavaRDD<String> usersLines = sc.textFile(basePath + "users.txt");
        String firstUserLine = usersLines.first();
        JavaPairRDD<String, String> userToOccRDD = usersLines.filter(line -> !line.equals(firstUserLine))
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    return new Tuple2<>(parts[0].trim(), parts[3].trim());
                });

        JavaRDD<String> ratingsLines = sc.textFile(basePath + "ratings_1.txt," + basePath + "ratings_2.txt");
        String firstRatingLine = ratingsLines.first();
        
        JavaPairRDD<String, Double> userRatingsRDD = ratingsLines.filter(line -> !line.equals(firstRatingLine))
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    return new Tuple2<>(parts[0].trim(), Double.parseDouble(parts[2].trim()));
                });

        JavaPairRDD<String, Tuple2<String, Double>> joinedRDD = userToOccRDD.join(userRatingsRDD);

        JavaPairRDD<String, Tuple2<Double, Integer>> occRatingPairs = joinedRDD.mapToPair(tuple -> 
            new Tuple2<>(tuple._2()._1(), new Tuple2<>(tuple._2()._2(), 1))
        );

        JavaPairRDD<String, Tuple2<Double, Integer>> reducedOcc = occRatingPairs.reduceByKey((v1, v2) ->
            new Tuple2<>(v1._1() + v2._1(), v1._2() + v2._2())
        );

        JavaPairRDD<String, Tuple2<Double, Integer>> finalStats = reducedOcc.mapValues(v -> 
            new Tuple2<>(v._1() / v._2(), v._2())
        );

        System.out.println(String.format("%-25s | %-12s | %s", "Nghe Nghiep", "Diem TB", "Luot Danh Gia"));

        finalStats.collect().forEach(result -> {
            String occId = result._1();
            Double avg = result._2()._1();
            Integer count = result._2()._2();
            String occName = broadcastOccNames.getValue().getOrDefault(occId, "Unknown (" + occId + ")");
            
            System.out.println(String.format("%-25s | %-12.2f | %d", occName, avg, count));
        });

        sc.stop();
    }
}