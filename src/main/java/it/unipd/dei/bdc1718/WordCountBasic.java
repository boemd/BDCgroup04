package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCountBasic {

    // The classic word count example, using data from a given file
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
        JavaRDD<String> docs = sc.textFile(path, numPartitions);

        JavaPairRDD<String, Long> wordcounts = docs
                .flatMapToPair((document) -> {             // <-- Map phase
                    String[] tokens = document.split(" ");
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        pairs.add(new Tuple2<>(token, 1L));
                    }
                    return pairs.iterator();
                })
                .groupByKey()                       // <-- Reduce phase
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return sum;
                });

        wordcounts.foreach((x) -> System.out.println(x._1 + " : " + x._2));
    }

}
