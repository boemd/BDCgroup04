package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Comparator;
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
        //If the Map function is an identity function, we omit it
        double mean = dNumbers.reduce((x, y) -> x + y) / dNumbers.count();
        System.out.println("Average: " + mean);

        // create a JavaRDD containing the absolute value of the difference between a number and the mean
        JavaRDD<Double> dDiffavgs = dNumbers.map((x) -> Math.abs(mean - x));
        //the method foreach(VoidFunction<T> f) allows us to pass in input a function whose return tipe is void
        //here we print the values contained in dDiffavgs
        //we do this because the dataset in very small
        dDiffavgs.foreach((x) -> System.out.println(x));

        // POINT 3
        // compute minimum using a map-reduce function
        double min1 = dDiffavgs.reduce((x, y) -> { if (x<=y) {return x;} else {return y;} });
        System.out.println("Minimum computed with method 1: " + min1);

        // compute minimum using min function
        double min2 = dDiffavgs.min(new DoubleComparator());
        System.out.println("Minimum computed with method 2: " + min2);

        // POINT 4
        // compute maximum using a map-reduce function
        double max = dDiffavgs.reduce((x, y) -> { if (x>=y){return x;} else {return y;} });
        System.out.println("Maximum: " + max);

        //sort the elements of dDiffavgs in ascending order
        JavaRDD<Double> dSorted = dDiffavgs.sortBy(x -> x, true,1);
        dSorted.foreach((x) -> System.out.println(x));

        //compute the variance of dNumbers
        double variance = dNumbers.map(x -> Math.pow(x-mean,2)).reduce((x, y) -> x + y) / (dNumbers.count()-1);
        System.out.println("Variance: " + variance);
    }

    // auxiliary class which allows Comparator to be Serializable
    public static class DoubleComparator implements Serializable, Comparator<Double> {
        public int compare(Double a, Double b) {
            if (a < b) return -1;
            else if (a > b) return 1;
            return 0;

            /*
            or, as an exotic alternative
            return a==b?0:a<b?-1:1;
            */
        } }
}