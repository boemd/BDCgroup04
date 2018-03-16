package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;

import java.io.*;
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
        // Computing the mean using a reduce (sum all elements and then divide by the cardinality) function
        // If the Map function is an identity function, we omit it
        double mean = dNumbers.reduce((x, y) -> x + y) / dNumbers.count();
        System.out.println("Average: " + mean);

        // Ccreate a JavaRDD containing the absolute value of the difference between a number and the mean
        JavaRDD<Double> dDiffavgs = dNumbers.map((x) -> Math.abs(mean - x));
        // The method foreach(VoidFunction<T> f) allows us to pass in input a function whose return type is void
        // here we print the values contained in dDiffavgs
        // We do this because the dataset in very small
        dDiffavgs.foreach((x) -> System.out.println(x));

        // POINT 3
        // Compute minimum using a map-reduce function
        double min1 = dDiffavgs.reduce((x, y) -> {
            if (x <= y) {
                return x;
            } else {
                return y;
            }
        });
        System.out.println("Minimum computed with method 1: " + min1);

        // Compute minimum using min function
        double min2 = dDiffavgs.min(new DoubleComparator());
        System.out.println("Minimum computed with method 2: " + min2);

        // POINT 4
        // Compute maximum (over numbers) using a map-reduce function
        double max = dNumbers.reduce((x, y) -> {
            if (x >= y) {
                return x;
            } else {
                return y;
            }
        });
        System.out.println("Maximum: " + max);

        // Sort the elements of dNumbers in ascending order
        JavaRDD<Double> dSorted = dNumbers.sortBy(x -> x, true, 1);
        dSorted.foreach((x) -> System.out.println(x));

        // Compute the variance of dNumbers
        double variance = dNumbers.map(x -> Math.pow(x - mean, 2)).reduce((x, y) -> x + y) / (dNumbers.count() - 1);
        System.out.println("Variance: " + variance);

        // Filtering to keep only numbers which are at most far mean+1
        JavaRDD<Double> dFiltered = dNumbers.filter((x) -> Math.abs(x - mean) < 1);
        dFiltered.foreach((x) -> System.out.println(x));

        // FileWriter and BufferedWriter initialization
        FileWriter d = null;
        try { d = new FileWriter("output.txt");
        }
        catch (IOException e) { System.err.println(e);
        }
        BufferedWriter w = new BufferedWriter(d);

        // Using collect to obtain iterable lists
        List<Double> numbers = dNumbers.collect();
        List<Double> diffAvgs = dDiffavgs.collect();
        List<Double> sorted = dSorted.collect();
        List<Double> filtered = dFiltered.collect();

        // Writing difference from mean mean to file
        try {
            w.write("-- POINT 2 --" + "\r\n");
            w.newLine();

            // Writing mean to file
            w.write("Mean of dataset is: " + mean + "\r\n");
            w.newLine();

            w.write("-- POINT 3 --" + "\r\n");
            w.newLine();

            // Writing difference wrt the mean
            w.write("Points and distance with respect to the mean are:" + "\r\n");
            for (int i = 0; i < diffAvgs.size(); i++) {
                w.write(numbers.get(i).toString() + " : " + diffAvgs.get(i).toString() + "\r\n");
            }
            w.newLine();

            // Writing min distance
            w.write("Minimum distance is: " +min1 + "\r\n");
            w.newLine();

            w.write("-- POINT 4 --" + "\r\n");
            w.newLine();

            // Writing max distance
            w.write("Maximum value in dataset is: " + max + "\r\n");
            w.newLine();

            // Writing sorted dataset (increasing order)
            w.write("Ordered dataset (increasing way) is:" + "\r\n");
            for (int i = 0; i < sorted.size(); i++) {
                w.write(sorted.get(i).toString() + "\r\n");
            }
            w.newLine();

            // Writing variance to file
            w.write("Variance of dataset is: " + variance + "\r\n");
            w.newLine();

            // Writing remaining numbers after filter
            w.write("Remaining numbers after filtering (and distance from the mean) are:" + "\r\n");
            for (int i = 0; i < filtered.size(); i++) {
                double a = filtered.get(i);
                w.write(a + " : " + Math.abs(a-mean) + "\r\n");
            }
            w.flush();
            d.close();
        }
        catch (IOException e) { System.err.println(e); }
    }

    // Auxiliary class which allows Comparator to be Serializable
    public static class DoubleComparator implements Serializable, Comparator<Double> {
        public int compare(Double a, Double b) {
            if (a < b) return -1;
            else if (a > b) return 1;
            return 0;

            /*
            Or, as an exotic alternative
            return a==b?0:a<b?-1:1;
            */
        } }
}