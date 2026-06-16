package com.baitap4;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class EcommerceDataFrameAnalysis {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Ecommerce DataFrame Analysis")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String basePath = "C:/Users/Administrator/Downloads/ds200_lab4/";

        System.out.println("========== LOADING DATA ==========");
        
        Dataset<Row> customersDF = spark.read().option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(basePath + "Customer_List.csv");
        Dataset<Row> orderItemsDF = spark.read().option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(basePath + "Order_Items.csv");
        Dataset<Row> reviewsDF = spark.read().option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(basePath + "Order_Reviews.csv");
        Dataset<Row> ordersDF = spark.read().option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(basePath + "Orders.csv");
        Dataset<Row> productsDF = spark.read().option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(basePath + "Products.csv");

        solveQ2(ordersDF, customersDF, orderItemsDF);
        solveQ3(ordersDF, customersDF);
        solveQ4(ordersDF);
        solveQ5(reviewsDF);
        solveQ6(ordersDF, orderItemsDF, productsDF);
        solveQ7(orderItemsDF, reviewsDF);
        solveQ8(ordersDF, orderItemsDF);

        spark.stop();
    }

    // --- QUESTION 2 ---
    private static void solveQ2(Dataset<Row> orders, Dataset<Row> customers, Dataset<Row> orderItems) {
        System.out.println("\n--- QUESTION 2: Overall Statistics ---");
        long totalOrders = orders.count();
        long totalCustomers = customers.select("Subscriber_ID").distinct().count();
        long totalSellers = orderItems.select("Seller_ID").distinct().count();

        System.out.println("Total orders: " + totalOrders);
        System.out.println("Total unique customers: " + totalCustomers);
        System.out.println("Total unique sellers: " + totalSellers);
    }

    // --- QUESTION 3 ---
    private static void solveQ3(Dataset<Row> orders, Dataset<Row> customers) {
        System.out.println("\n--- QUESTION 3: Orders by Country ---");
        Dataset<Row> result = customers.join(orders, "Customer_Trx_ID")
                .groupBy("Customer_Country")
                .agg(count("Order_ID").alias("Total_Orders"))
                .orderBy(col("Total_Orders").desc());
        result.show(10);
    }

    // --- QUESTION 4 ---
    private static void solveQ4(Dataset<Row> orders) {
        System.out.println("\n--- QUESTION 4: Orders by Year and Month ---");
        Dataset<Row> result = orders
                .withColumn("Year", year(col("Order_Purchase_Timestamp")))
                .withColumn("Month", month(col("Order_Purchase_Timestamp")))
                .groupBy("Year", "Month")
                .agg(count("Order_ID").alias("Total_Orders"))
                .orderBy(col("Year").asc(), col("Month").desc());
        result.show(10);
    }

    // --- QUESTION 5 ---
    private static void solveQ5(Dataset<Row> reviews) {
        System.out.println("\n--- QUESTION 5: Review Score Statistics ---");
        Dataset<Row> cleanedReviews = reviews
                .filter(col("Review_Score").isNotNull())
                .filter(col("Review_Score").between(1, 5));

        Row avgRow = cleanedReviews.select(avg("Review_Score")).first();
        System.out.printf("System Overall Average Review Score: %.2f\n", avgRow.getDouble(0));

        Dataset<Row> scoreCounts = cleanedReviews.groupBy("Review_Score")
                .agg(count("Review_ID").alias("Count"))
                .orderBy(col("Review_Score").asc());
        scoreCounts.show();
    }

    // --- QUESTION 6 ---
    private static void solveQ6(Dataset<Row> orders, Dataset<Row> orderItems, Dataset<Row> products) {
        System.out.println("\n--- QUESTION 6: 2024 Revenue by Product Category ---");
        Dataset<Row> revenueDF = orderItems.join(orders, "Order_ID")
                .join(products, "Product_ID")
                .filter(year(col("Order_Purchase_Timestamp")).equalTo(2024))
                .withColumn("Revenue", col("Price").plus(col("Freight_Value")))
                .groupBy("Product_Category_Name")
                .agg(sum("Revenue").alias("Total_Revenue"))
                .orderBy(col("Total_Revenue").desc());
        revenueDF.show(10);
    }

    // --- QUESTION 7 ---
    private static void solveQ7(Dataset<Row> orderItems, Dataset<Row> reviews) {
        System.out.println("\n--- QUESTION 7: Top Selling Products ---");
        Dataset<Row> topProduct = orderItems.groupBy("Product_ID")
                .agg(count("Order_Item_ID").alias("Total_Sold"))
                .orderBy(col("Total_Sold").desc());
        topProduct.show(5);

        System.out.println("Average Review Score per Product:");
        Dataset<Row> cleanedReviews = reviews.filter(col("Review_Score").isNotNull());
        Dataset<Row> productAvgScore = orderItems.join(cleanedReviews, "Order_ID")
                .groupBy("Product_ID")
                .agg(avg("Review_Score").alias("Avg_Review_Score"));
        productAvgScore.show(5);
    }

    // --- QUESTION 8 ---
    private static void solveQ8(Dataset<Row> orders, Dataset<Row> orderItems) {
        System.out.println("\n--- QUESTION 8: Delivery Performance (Days Difference) ---");
        Dataset<Row> result = orderItems.join(orders, "Order_ID")
                .filter(col("Order_Delivered_Carrier_Date").isNotNull().and(col("Shipping_Limit_Date").isNotNull()))
                .withColumn("Delivery_Diff_Days", datediff(col("Order_Delivered_Carrier_Date"), col("Shipping_Limit_Date")))
                .select("Order_ID", "Order_Delivered_Carrier_Date", "Shipping_Limit_Date", "Delivery_Diff_Days");
        result.show(10);
    }
}