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
    public static void main(String[] args){

        // ------------------------------ POINT 1 ------------------------------

        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }
        String path = args[0];

        // Setup Spark
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load a text file into an RDD of strings, where each string corresponds to a distinct line (document) of the
        // file
        int numPartitions = sc.defaultParallelism();

        // ----------------------- IMPROVED WORD COUNT 1 -----------------------

        JavaRDD<String> lines = sc.textFile(path, numPartitions).cache();   // Added .cache() to force the loading of
                                                                            // the file before the stopwatch is started
        lines.count();
        // Now the RDD has been loaded and cached in memory and we can start measuring time
        long start = System.currentTimeMillis();
        JavaPairRDD<String,Long> docs = lines.flatMapToPair((document)-> {
            // I split each document in words and I count the repetitions in the document
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token : tokens) {
                //I iterate on the list to see if the current token has already been added
                ListIterator<Tuple2<String, Long>> itr = pairs.listIterator();
                boolean done=false;
                while(itr.hasNext()){
                    Tuple2<String,Long> elem = itr.next();
                    //if the token is present, its value gets incremented by 1
                    if(elem._1().equals(token)){
                        itr.set(new Tuple2<>(elem._1(),elem._2()+1L));
                        done=true;
                        break;
                    }
                }
                //if the token is not found, I add it with value 1
                if(!done){
                    pairs.add(new Tuple2<>(token, 1L));
                }
            }
            return pairs.iterator();
        })
        .groupByKey()
        .mapValues((it)-> {
            long sum = 0;
            for (long c : it)
                sum += c;
            return sum;
        })
        .sortByKey();

        long end = System.currentTimeMillis();
        System.out.println("Elapsed time of Improved Word Count 1: " + (end - start) + " ms");

        // Output of Improved Word Count 1, not required

//        List<Tuple2<String, Long>> counts = docs.collect();
//
//        counts.forEach((tuple) -> {
//            String word = tuple._1();
//            long count = tuple._2();
//            System.out.println(word + " :: " + count);
//        });

        // ----------------------- IMPROVED WORD COUNT 2 -----------------------

    }
}
