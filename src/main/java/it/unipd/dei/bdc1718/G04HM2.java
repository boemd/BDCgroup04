package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

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

        // Load a text file into an RDD of strings, where each string corresponds to a distinct line (document) of the file
        int numPartitions = sc.defaultParallelism();

        // ----------------------- IMPROVED WORD COUNT 1 -----------------------

        JavaRDD<String> lines = sc.textFile(path, numPartitions).cache();   // Added .cache() to force the loading of
        lines.count();                                                      // the file before the stopwatch is started

        // Now the RDD has been loaded and cached in memory and we can start measuring time
        long start = System.currentTimeMillis();

        JavaPairRDD<String, Long> ImprWC1 = lines.flatMapToPair((document) -> {
            // I split each document in words and I count the repetitions in the document
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token : tokens) {
                // I iterate on the list to see if the current token has already been added
                ListIterator<Tuple2<String, Long>> itr = pairs.listIterator();
                boolean done = false;
                while (itr.hasNext()) {
                    Tuple2<String, Long> elem = itr.next();
                    // If the token is present, its value gets incremented by 1
                    if (elem._1().equals(token)) {
                        //I use the method set because I need to substitute the old Tuple
                        itr.set(new Tuple2<>(elem._1(), elem._2() + 1L));
                        done = true;
                        break;
                    }
                }
                // If the token is not found, I add it with value 1
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
        ImprWC1.count();
        // Need to sort the elements
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
        long word_occurrences = lines2.count();
        long sqrtN = (long) Math.sqrt(word_occurrences);     // We need a key which is a random value in [0,sqrtN)

        start = System.currentTimeMillis();

        JavaPairRDD<String,Long> ImprWC2 = lines2.flatMapToPair((document) -> {
            // I split each document in words and I count the repetitions in the document
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<Long, Tuple2<String, Long>>> triplet = new ArrayList<>();
            for (String token : tokens) {
                // I iterate on the list to see if the current token has already been added
                ListIterator<Tuple2<Long, Tuple2<String, Long>>> itr = triplet.listIterator();
                boolean done = false;
                while (itr.hasNext()) {
                    Tuple2<Long, Tuple2<String, Long>> elem = itr.next();
                    // If the token is present, its value gets incremented by 1
                    if (elem._2._1().equals(token)) {
                        // I add a tuple of the type [key_of_the_pair, (word, incremented_value)]
                        itr.set(new Tuple2<>(elem._1(), new Tuple2<>(elem._2._1(), elem._2._2() + 1L)));
                        done = true;
                        break;
                    }
                }
                // If the token is not found, I add it with value 1
                if (!done) {
                    long xKey = (long) (Math.random() * (sqrtN));        // Numero random da 0 a sqrt(N)
                    triplet.add(new Tuple2<>(xKey, new Tuple2<>(token, 1L)));
                }
            }
            return triplet.iterator();
        })
        .groupByKey()
        .flatMapToPair((triplet) -> {
            ArrayList<Tuple2<String,Long>> tbr = new ArrayList<>(); //list to be returned: here I put the words and the number of occurrences
            //Iterator<Tuple2<String, Long>> triplets_iterator = triplet._2().iterator(); //iterator over the gathered pairs
            //iteration over the gathered pairs
            //while (triplets_iterator.hasNext()){
            //    Tuple2<String, Long> current = triplets_iterator.next();
            for(Tuple2<String, Long> current : triplet._2){
                ListIterator<Tuple2<String,Long>> iterTBR = tbr.listIterator();// iterator on the list tbr
                boolean done = false;
                while(iterTBR.hasNext()){
                    Tuple2<String,Long> elemTBR = iterTBR.next();
                    //if the tuples have the same word
                    if(elemTBR._1().equals(current._1())){
                        iterTBR.set(new Tuple2<>(elemTBR._1(),elemTBR._2()+current._2()));
                        done = true;
                        break;
                    }
                }
                if (!done){
                    iterTBR.add(new Tuple2<>(current._1(),current._2()));
                }
            }
            return tbr.iterator();
        }).groupByKey()              // For each word we gather the, at most, sqrtN pairs (word, count) and we produce
         .mapValues((it) -> {        // the pair (word, countTot)
             long sum = 0;
             for (long c : it)
                 sum += c;
             return sum;
             });

        ImprWC2.count();
        end = System.currentTimeMillis();
        System.out.println("Elapsed time of Improved Word Count 2: " + (end - start) + " ms");


        // -------------------- IMPROVED WORD COUNT WITH reduceByKey() --------------------

        JavaRDD<String> lines3 = sc.textFile(path, numPartitions).cache();
        lines3.count();

        start = System.currentTimeMillis();

        JavaPairRDD<String, Long> ImprWC3 = lines3.flatMapToPair((document) -> {
            // I split each document in words and I count the repetitions in the document
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
            for (String token : tokens) {
                // I iterate on the list to see if the current token has already been added
                ListIterator<Tuple2<String, Long>> itr = pairs.listIterator();
                boolean done = false;
                while (itr.hasNext()) {
                    Tuple2<String, Long> elem = itr.next();
                    // If the token is present, its value gets incremented by 1
                    if (elem._1().equals(token)) {
                        itr.set(new Tuple2<>(elem._1(), elem._2() + 1L));
                        done = true;
                        break;
                    }
                }
                // If the token is not found, I add it with value 1
                if (!done) {
                    pairs.add(new Tuple2<>(token, 1L));
                }
            }
            return pairs.iterator();
        })
        .reduceByKey((x,y) -> x+y);
        ImprWC3.count();
        end = System.currentTimeMillis();
        System.out.println("Elapsed time of Word Count with reduceByKey(): " + (end - start) + " ms");


        // ----------------------- IMPROVED IMPROVED WORD COUNT 2 -----------------------
        // As the title says, this is an improvement of Improved Word Count 2.
        // In the first round, instead of grouping data as (x,(w, Ci(w)))
        // data is grouped as ((x,w), Ci(w)) and then reduceByKey() is called twice
        // This implementation is one round longer than the one seen in class (and implemented in IMPROVED WORD COUNT 2)
        // since it's necessary to get rid of the random value x (map phase) and then perform the final count (reduce phase),
        // but the execution time is quite lower.

        JavaRDD<String> linesf = sc.textFile(path, numPartitions).cache();
        long word_occurrencesf = linesf.count();
        long sqrtNf = (long) Math.sqrt(word_occurrencesf);     // We need a key which is a random value in [0,sqrtN)

        start = System.currentTimeMillis();

        JavaPairRDD<String,Long> ImprWCf = linesf.flatMapToPair((document) -> {
            // I split each document in words and I count the repetitions in the document
            String[] tokens = document.split(" ");
            ArrayList<Tuple2<Tuple2<Long,String>, Long>> triplet = new ArrayList<>();
            for (String token : tokens) {
                // I iterate on the list to see if the current token has already been added
                ListIterator<Tuple2<Tuple2<Long,String>, Long>> triplets_iterator = triplet.listIterator();
                boolean done = false;
                while (triplets_iterator.hasNext()) {
                    Tuple2<Tuple2<Long,String>, Long> elem = triplets_iterator.next();
                    // If the token is present, its value gets incremented by 1
                    if (elem._1._2().equals(token)) {
                        triplets_iterator.set(new Tuple2<>(new Tuple2<>(elem._1()._1(),elem._1()._2()), elem._2()+1L)); //sorry guys for the language of gods
                        done = true;
                        break;
                    }
                }
                // If the token is not found, I add it with value 1
                if (!done) {
                    long xKey = (long) (Math.random() * (sqrtNf));        // Numero random da 0 a sqrt(N)
                    triplet.add(new Tuple2<>(new Tuple2<>(xKey,token), 1L));
                }
            }
            return triplet.iterator();
        })
        .reduceByKey((x,y) -> x+y)
        .mapToPair((triplet) -> new Tuple2<>(triplet._1()._2(), triplet._2()))
        .reduceByKey((x,y) -> x+y);
        ImprWCf.count();
        end = System.currentTimeMillis();
        System.out.println("Elapsed time of the Improved Improved Word Count 2: " + (end - start) + " ms");




        /////////////////////////////////////////////POINT 2//////////////////////////////////////////

        System.out.println("Please type the number of most frequent words you want to see:");
        Scanner in = new Scanner(System.in);
        int k;
        k=in.nextInt();
        while(k<1){
            System.out.println("Please insert a positive value:");
            k=in.nextInt();
        }
        in.close();
        System.out.println("The " + k + " most frequent words in " + path + " are:");

        JavaPairRDD<Long, String> frequentwords = ImprWCf
        .mapToPair((tuple) -> new Tuple2<>(tuple._2, tuple._1))
        .sortByKey(false);                                          // sortByKey() performs sorting in an ascending order, this one in descending order

        List<Tuple2<Long, String>> counts = frequentwords.collect();
        Iterator<Tuple2<Long, String>> iter = counts.iterator();
        for(int i=0; i<k; i++){
            Tuple2<Long, String> tuple = iter.next();
            String word = tuple._2();
            long count = tuple._1();
            System.out.println(word + " :: " + count);
        }
    }
}