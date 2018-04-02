package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Serializable;
import scala.Tuple2$mcCC$sp;

import java.util.*;
import java.io.*;

public class G04HM2 {
    public static void main(String[] args) {

        /////////////////////////////////////////////POINT 1//////////////////////////////////////////

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

        JavaRDD<String> lines1 = sc.textFile(path, numPartitions).cache();   // Added .cache() to force the loading of
        lines1.count();                                                      // the file before the stopwatch is started

        // Now the RDD has been loaded and cached in memory and we can start measuring time
        long start = System.currentTimeMillis();

        JavaPairRDD<String, Long> ImprWC1 = lines1.flatMapToPair((document) -> {
            // I split each document in words and I count the repetitions in the document
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token : tokens) {
                //I iterate on the list to see if the current token has already been added
                ListIterator<Tuple2<String, Long>> itr = pairs.listIterator();
                boolean done = false;
                while (itr.hasNext()) {
                    Tuple2<String, Long> elem = itr.next();
                    //if the token is present, its value gets incremented by 1
                    if (elem._1().equals(token)) {
                        itr.set(new Tuple2<>(elem._1(), elem._2() + 1L));
                        done = true;
                        break;
                    }
                }
                //if the token is not found, I add it with value 1
                if (!done) {
                    pairs.add(new Tuple2<>(token, 1L));
                }
            }
            return pairs.iterator();
        })
                .groupByKey()
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it)
                        sum += c;
                    return sum;
                });
        //need to sort the elements
        //JavaRDD<Tuple2<String, Long>> wordCounts = docs.toRDD().sortBy((tuple) -> tuple._2(), true, numPartitions);

        long end = System.currentTimeMillis();
        System.out.println("Elapsed time of Improved Word Count 1: " + (end - start) + " ms");

        // Output of Improved Word Count 1, not required
        /*
        List<Tuple2<String, Long>> counts = ImprWC1.collect();
          counts.forEach((tuple) -> {
            String word = tuple._1();
            long count = tuple._2();
            System.out.println(word + " :: " + count);
        });
        */

        // ----------------------- IMPROVED WORD COUNT 2 -----------------------

        JavaRDD<String> lines2 = sc.textFile(path, numPartitions).cache();
        // long word_occurrences = 3503570;
        long word_occurrences = lines2.count();
        long sqrtN = (long) Math.sqrt(word_occurrences);     // We need a key which is a random value in [0,sqrtN)

        start = System.currentTimeMillis();

        // Da modificare il tipo di ImprWC2 in JavaPairRDD<String, Long>; l'avevo messo così com'è attualmente per fare
        // in modo che non venisse fuori tutto rosso quello che era all'interno del primo flatMapToPair()
        JavaPairRDD<Long, java.lang.Iterable<scala.Tuple2<String, Long>>> ImprWC2 = lines2.flatMapToPair((document) -> {
            // I split each document in words and I count the repetitions in the document
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<Long, Tuple2<String, Long>>> triplet = new ArrayList<>();
            for (String token : tokens) {
                //I iterate on the list to see if the current token has already been added
                ListIterator<Tuple2<Long, Tuple2<String, Long>>> itr = triplet.listIterator();
                boolean done = false;
                while (itr.hasNext()) {
                    Tuple2<Long, Tuple2<String, Long>> elem = itr.next();
                    //if the token is present, its value gets incremented by 1
                    if (elem._2._1().equals(token)) {
                        Tuple2<String, Long> supp = new Tuple2<>(elem._2._1(), elem._2._2() + 1L);
                        itr.set(new Tuple2<>(elem._1(), new Tuple2<>(supp._1(), supp._2())));
                        done = true;
                        break;
                    }
                }
                //if the token is not found, I add it with value 1
                if (!done) {
                    long xKey = (long) (Math.random() * (sqrtN));        // Numero random da 0 a sqrt(N)
                    triplet.add(new Tuple2<>(xKey, new Tuple2<>(token, 1L)));
                }
            }
            return triplet.iterator();
        })
        .groupByKey()   // Questa è la prima parte del Reduce nel Round 1 (slide 22)
        /*.flatMapToPair((triplet) -> {
            ArrayList<Tuple2<String, Long>> pair = new ArrayList<>();
            // IDEA PER QUESTA PARTE
            // Si passano tutte le Key-Value pairs di triplet (penso serva un modo per contare il numero di xKey e poi
            // far partire un for), che in realtà è nella forma < xKey, Iterable<Tuple> >
            // Con la tupla interna che è nella forma < Word, CountParziale >
            // Se si trova un doppione di Word si sommano i 2 CountParziale e si va avanti così
            // Alla fine si ritorna un nuovo RDD senza xKey e nella forma < Word, CountTot >
        })
        .groupByKey()               // For each word we gather the, at most, sqrtN pairs (word, count) and we produce
        .mapValues((it) -> {        // the pair (word, countTot)
            long sum = 0;
            for (long c : it)
                sum += c;
            return sum;
            })*/;

        end = System.currentTimeMillis();
        System.out.println("Elapsed time of Improved Word Count 2: " + (end - start) + " ms");

        // -------------------- WORD COUNT WITH reduceByKey --------------------

        JavaRDD<String> lines3 = sc.textFile(path, numPartitions).cache();
        lines3.count();

        start = System.currentTimeMillis();

        JavaPairRDD<String, Long> WC3 = lines3.flatMapToPair((document) -> {
            // I split each document in words and I count the repetitions in the document
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token : tokens) {
                //I iterate on the list to see if the current token has already been added
                ListIterator<Tuple2<String, Long>> itr = pairs.listIterator();
                boolean done = false;
                while (itr.hasNext()) {
                    Tuple2<String, Long> elem = itr.next();
                    //if the token is present, its value gets incremented by 1
                    if (elem._1().equals(token)) {
                        itr.set(new Tuple2<>(elem._1(), elem._2() + 1L));
                        done = true;
                        break;
                    }
                }
                //if the token is not found, I add it with value 1
                if (!done) {
                    pairs.add(new Tuple2<>(token, 1L));
                }
            }
            return pairs.iterator();
        })
                .reduceByKey((x,y) -> x+y);

        end = System.currentTimeMillis();
        System.out.println("Elapsed time of Word Count with reduceByKey(): " + (end - start) + " ms");

        /////////////////////////////////////////////POINT 2//////////////////////////////////////////

        System.out.println("Please type the number of most frequent words you want to see.");
        Scanner in = new Scanner(System.in);
        int k;
        k=in.nextInt();
        while(k<1){
            System.out.println("Please insert a positive value.");
            k=in.nextInt();
        }
        in.close();
        System.out.println("The " + k + " most frequent words in " + path + " are:");

        JavaPairRDD<String, Long> frequentwords = ImprWC1.mapToPair((tuple) -> {
            String word = tuple._1();
            long count = tuple._2();
            return new Tuple2<>(count, word);
        })
        .sortByKey(false)                   // .sortbyKey() sort in an ascending order, this one in descending order
        .mapToPair((tuple) -> {
            String word = tuple._2();
            long count = tuple._1();
            return new Tuple2<>(word, count);
        });

        List<Tuple2<String, Long>> counts = frequentwords.collect();
        Iterator<Tuple2<String, Long>> iter = counts.iterator();
        for(int i=0; i<k; i++){
            Tuple2<String, Long> tuple = iter.next();
            String word = tuple._1();
            long count = tuple._2();
            System.out.println(word + " :: " + count);
        }

    }
}
