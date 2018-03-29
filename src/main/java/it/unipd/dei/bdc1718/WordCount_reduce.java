package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class WordCount_reduce {

    public static void main(String[] args) {

        // Parse the path to the data
        String path = args[0];

        // Initialize Spark
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf sparkConf = new SparkConf(true).setAppName("Basic Word Count");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Read the data from the text file
        int numPartitions = sc.defaultParallelism();
        JavaRDD<String> dDocs = sc.textFile(path, numPartitions);

        // Iterator
        JavaRDD<String> dWords = dDocs.flatMap((doc) -> Arrays.stream(doc.split(" ")).iterator());

        // new JavaPairRDD to store (key, value) pairs
        // Each word is mapped into a tuple (word, 1)
        // Then using reduceByKey we gather together all pairs with same key and increase value
        JavaPairRDD<String, Integer> dCounts = dWords.mapToPair((w) -> new Tuple2<>(w, 1)).reduceByKey((x, y) -> x + y);

        // Swap (key, value) to (value, key) to use sortByKey
        JavaPairRDD<Integer, String> dSwapped = dCounts.mapToPair((w) -> new Tuple2<>(w._2, w._1));
        dSwapped = dSwapped.sortByKey(true, 1);

        // Output is word : occurrences
        dSwapped.foreach( x -> System.out.println(x._2 + " : " + x._1));
    }

}
