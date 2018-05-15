package it.unipd.dei.bdc1718;

import org.apache.commons.collections.iterators.ArrayListIterator;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ListIterator;
import java.util.Scanner;

public class G04HM4 {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }
        String path = args[0];

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        SparkConf conf = new SparkConf(true).setAppName("Fourth Homework");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load a text file into an RDD of strings, where each string corresponds to a distinct line (document) of the file
        int numPartitions = sc.defaultParallelism();

        // Create JavaRDD from input path
        JavaRDD<Vector> pointsrdd = InputOutput.readVectors(sc,path);
    }

    /**
     * First method required by the assignment
     * runMapReduce(pointsrdd, k, numBlocks)
     * A method runMapReduce(pointsrdd,k,numBlocks) that receives in input a set of points represented by a JavaRDD<Vector>
     * pointsrdd and two integers k and numBlocks, and does the following things:
     * (a) partitions pointsrdd into numBlocks subsets;
     * (b) extracts k points from each subset by running the sequential Farthest-First Traversal algorithm implemented for
     *      Homework 3;
     * (c) gathers the numBlocks*k points extracted into an ArrayList<Vector> coreset;
     * (d) returns an ArrayList<Vector> object with k points determined by running the sequential max-diversity algorithm
     *      with input coreset and k. The code of the sequential algorithm can be downloaded here.
     */

    static ArrayList<Vector> runMapReduce(JavaRDD<Vector> pointsrdd, int k, int numBlocks) {
        // (a)
        // (b)
        // (c)
        // (d)
        return null;
    }

    /**
     * Second method required by the assignment
     * measure(pointslist)
     */
    static double measure(ArrayList<Vector> pointslist) {

        int numPoints = pointslist.size();
        double sum = 0;

        for(int i=0; i<numPoints; i++){
            for(int j=i+1; j<numPoints; j++){
                sum += Math.sqrt(Vectors.sqdist(pointslist.get(i),pointslist.get(j)));
            }
        }
        return sum/(numPoints*(numPoints-1)/2);
    }

    /**
     * Sequential approximation algorithm based on matching provided by the link in the assignment.
     */
    public static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {
        final int n = points.size();
        if (k >= n) {
            return points;
        }

        ArrayList<Vector> result = new ArrayList<>(k);
        boolean[] candidates = new boolean[n];
        Arrays.fill(candidates, true);
        for (int iter=0; iter<k/2; iter++) {
            // Find the maximum distance pair among the candidates
            double maxDist = 0;
            int maxI = 0;
            int maxJ = 0;
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    for (int j = i+1; j < n; j++) {
                        if (candidates[j]) {
                            double d = Math.sqrt(Vectors.sqdist(points.get(i), points.get(j)));
                            if (d > maxDist) {
                                maxDist = d;
                                maxI = i;
                                maxJ = j;
                            }
                        }
                    }
                }
            }
            // Add the points maximizing the distance to the solution
            result.add(points.get(maxI));
            result.add(points.get(maxJ));
            // Remove them from the set of candidates
            candidates[maxI] = false;
            candidates[maxJ] = false;
        }
        // Add an arbitrary point to the solution, if k is odd.
        if (k % 2 != 0) {
            for (int i = 0; i < n; i++) {
                if (candidates[i]) {
                    result.add(points.get(i));
                    break;
                }
            }
        }
        if (result.size() != k) {
            throw new IllegalStateException("Result of the wrong size");
        }
        return result;
    }
}





