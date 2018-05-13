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

    static double measure(ArrayList<Vector> pointslist){

        int numPoints = pointslist.size();
        double sum = 0;

        for(int i=0; i<numPoints; i++){
            for(int j=i+1; j<numPoints; j++){
                sum += Math.sqrt(Vectors.sqdist(pointslist.get(i),pointslist.get(j)));
            }
        }
        return sum/(numPoints*(numPoints-1)/2);
    }
}




