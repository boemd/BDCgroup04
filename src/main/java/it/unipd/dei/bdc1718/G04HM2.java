/*

Assignment
Create a program GxxHM2.java (for Java users) or GxxHM2.py (for Python users), where xx is your two-digit group number,
which receives in input a collection of documents, represented as a text file (one line per document) whose name is
provided on the command line, and does the following things:

1. Runs 3 versions of MapReduce word count and returns their individual running times, carefully measured:
    - a version that implements the Improved Word count 1 described in class.
    - a version that implements the Improved Word count 2 described in class.
    - a version that uses the reduceByKey method.
Try to make each version as fast as possible. You can test it on the text-sample.txt file you downloaded earlier or
even on a much larger file you can create yourself.

2. Asks the user to input an integer k and returns the k most frequent words (i.e., those with largest counts), with
ties broken arbitrarily.
Add short but explicative comments to your code and when you print a value print also a short description of what that
value is.

Return the file with your program by mail to bdc-course@dei.unipd.it

*/

package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Serializable;
import java.util.*;
import java.io.*;

public class G04HM2 {
    public static void main(String[] args) throws FileNotFoundException {

        // ---------------------CODICE DI DARIO--------------------------

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
