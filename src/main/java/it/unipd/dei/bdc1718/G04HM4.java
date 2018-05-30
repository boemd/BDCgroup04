package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

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

        int numBlocks, k;

        Scanner in = new Scanner(System.in);
        System.out.println("Insert the value of numBlocks and k.");
        System.out.println("Insert the value of numBlocks:");
        numBlocks = in.nextInt();
        System.out.println("Insert the value of k:");
        k = in.nextInt();
        in.close();

        // Create JavaRDD from input path
        JavaRDD<Vector> pointsrdd = InputOutput.readVectors(sc,path).cache();
        pointsrdd.count();

        ArrayList<Vector> out = runMapReduce(pointsrdd, k, numBlocks);

        double dst = measure(out);
        System.out.println("The average distance among the solution points is: " + dst);
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
        // (a-b)
        Random r = new Random();
        long t0 = System.currentTimeMillis();
        List<ArrayList<Vector>> a = pointsrdd.mapToPair( (vector) -> (new Tuple2<Integer, Vector> (r.nextInt(numBlocks), vector)))
                .groupByKey() // es 1A
                .mapToPair( (it) -> {
                    ArrayList<Vector> v = new ArrayList<>();
                    for (Vector vec : it._2)
                    { v.add(vec); }
                    ArrayList<Vector> kcen = kcenter(v,k);
                    return new Tuple2<>(0,kcen);
                })
                .map( (tupla) -> tupla._2)
                .collect();

        // (c)
        ArrayList<Vector> coreset = new ArrayList<>();

        ListIterator<ArrayList<Vector>> iter = a.listIterator();
        while(iter.hasNext()){
            ListIterator ali = iter.next().listIterator();
            while(ali.hasNext()){
                Vector v = (Vector) ali.next();
                coreset.add(v);
            }
        }
        long t1 = System.currentTimeMillis();
        System.out.println("Time taken by the coreset construction: " + (t1-t0) + " ms.");

        // (d)
        long t2 = System.currentTimeMillis();
        ArrayList<Vector> finalClustering = runSequential(coreset, k);
        long t3 = System.currentTimeMillis();
        System.out.println("Time taken by the computation of the final solution: " + (t3-t2) + " ms.");
        return finalClustering;
    }

    /**
     * Second method required by the assignment
     * measure(pointslist)
     * A method measure(pointslist) that receives in input a set of points represented by an ArrayList<Vector>
     * pointslist and returns a double which is the average distance between all points in pointslist
     */
    private static double measure(ArrayList<Vector> pointslist) {

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
    private static ArrayList<Vector> runSequential(final ArrayList<Vector> points, int k) {
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

    /**
     * Farthest - First Traversal Algorithm
     */
    private static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k) {
        if (k <= 0) {
            return null;
        }

        // ArrayList in which to put the centers
        ArrayList<Vector> S = new ArrayList<>();

        // Choose a random point on P, remove it and put it in S
        int n = (int) (Math.random() * (P.size() - 1)); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);        // IMPORTANT: we call this P but we use this as P\S


        // ArrayList in which to put all distances between P\S and S
        ArrayList<Double> minDistPS = new ArrayList<>();
        // Initialization of the List, it will be updated with complexity O(|P|) in the "for" loop
        for(int i = 0; i < P.size(); i++){
            minDistPS.add(Double.MAX_VALUE);
        }


        for (int i = 0; i < k-1; i++) {

            // At the beginning of each iteration, the iterator points at the beginning of the ArrayList P
            ListIterator<Vector> iterP = P.listIterator();

            // Iterator on minDistPS
            ListIterator<Double> iterDist = minDistPS.listIterator();

            // Update minDistPS
            Vector s = S.get(0); //the last point added to S
            while(iterP.hasNext()){                             // for each element of P
                double tmp = Vectors.sqdist(iterP.next(), s);   // distance of the current element of P and the last element added in S (which has position 0)
                if (tmp < iterDist.next()){                     // if the new distance is lower than the old distance
                    iterDist.set(tmp);                          // the new distance replaces the old one
                }
            }

            // Find the index of the point of P\S with higher distance from S
            int maxIndex = maxIdx(minDistPS);

            // This part is needed to add the vector from P to S
            int psize = P.size();
            ListIterator itrP = P.listIterator();
            ListIterator itrS = S.listIterator();
            ListIterator distPS = minDistPS.listIterator();
            for (int h = 0; h < psize; h++) {   // "for" loop to find the vector of P\S which is in the position maxIndex
                Vector v = (Vector) itrP.next();
                distPS.next();
                if (h == maxIndex){
                    itrP.remove();
                    distPS.remove();
                    itrS.add(v);                // Since we haven't called yet itrS.next(), v is added to the position 0
                }

            }
        }
        return S;
    }

    /**
     * Support method for kcenter
     */
    private static int maxIdx(ArrayList<Double> S) {
        int maxIdx = -1;
        double max = Double.MIN_VALUE;
        ListIterator<Double> iterS = S.listIterator();
        for (int i = 0; iterS.hasNext(); i++) {
            double tmp = iterS.next();
            if (tmp > max) {
                max = tmp;
                maxIdx = i;
            }
        }
        return maxIdx;
    }
}