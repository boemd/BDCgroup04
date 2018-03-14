package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

public class G04HM1 {
    public static void main(String[] args) throws FileNotFoundException {
        //  POINT 1
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }
        // Read a list of numbers from the program options
        ArrayList<Double> lNumbers = new ArrayList<>();
        Scanner s = new Scanner(new File(args[0]));
        while (s.hasNext()) {
            lNumbers.add(Double.parseDouble(s.next()));
        }
        s.close();

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Create a parallel collection
        JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);

        // POINT 2
        // In the variable sum I store the sum of the mean of the elements of the data set
        double mean = dNumbers.reduce((x, y) -> x + y) / dNumbers.count();
        System.out.println("Average: " + mean);

        // create a JavaRDD containing the absolute value of the difference between a number and the mean
        JavaRDD<Double> dDiffavgs = dNumbers.map((x) -> Math.abs(mean - x));
        dDiffavgs.foreach((x) -> System.out.println(x));

        // POINT 3
        // compute minimum using a map-reduce function
        double min1 = dDiffavgs.reduce((x, y) -> { if (x<=y) {return x;} else {return y;} });
        System.out.println("Minimum computed with method 1: " + min1);

        // compute minimum using min function
        double min2 = dDiffavgs.min(new DoubleComparator());
        System.out.println("Minimum computed with method 2: " + min2);

        // POINT 4
        // Don't know what to do :(
    }

    // auxiliary class which allows Comparator to be Serializable
    public static class DoubleComparator implements Serializable, Comparator<Double> {
        public int compare(Double a, Double b) {
            if (a<b) return -1;
            else if (a>b) return 1;
            return 0; }
    }
}