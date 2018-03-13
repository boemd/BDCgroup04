package it.unipd.dei.bdc1718;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.io.FileWriter;

public class ComputePrimes {

    // As a first examples, we will compute the first primes
    // included in the first k natural numbers, with k given
    // on the command line
    public static void main(String[] args) throws IOException {

        // First, we parse k from the command line
        int k = Integer.parseInt(args[0]);

        // Then, we initialize Spark.
        // We need to set a master on the command line, like
        //
        //     -Dspark.master="local[4]"
        //
        // to run Spark using 4 cores
        SparkConf sparkConf = new SparkConf(true).setAppName("Compute primes");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Then, we instantiate a _local_ collection of the first k integers
        List<Integer> naturals = new ArrayList<>(k);
        for (int i=0; i<k; i++) {
            naturals.add(i);
        }

        // We now use Spark to get a parallel version of our collection.
        // you can configure the parallelism on the command line using by
        // setting the relevant property with
        //
        //     -Dspark.default.parallelism=8
        //
        int numPartitions = sc.defaultParallelism();
        System.err.println("Splitting data in " + numPartitions + " partitions");
        JavaRDD<Integer> dNaturals = sc.parallelize(naturals, numPartitions);

        // We can use parallel transformations on our
        // distributed collection to get _new_ collections,
        // similarly to what we saw in the first lesson
        //
        // This first transformation wraps the integers in a BigInteger
        JavaRDD<BigInteger> dBigInts = dNaturals.map((x) -> {
            // Uncomment the print statement below if you want to get a
            // message from each invocation, for instance to see if the result
            // is being recomputed.
            //System.out.println("[map] Wrapping " + x + " into a BigInteger");
            return BigInteger.valueOf(x);
        });

        // Now we can filter the numbers that are prime
        JavaRDD<BigInteger> dPrimes = dBigInts
                .filter((x) -> x.isProbablePrime(10000)) // Filter number that are prime
                .cache(); // Cache the result. Remove this line if you want to see the recomputation

        // Count the number of primes
        long numPrimes = dPrimes.count();
        // Actually collect the primes. Note that this does not
        // trigger a new computation only if we cached the result.
        List<BigInteger> primes = dPrimes.collect();
        FileWriter a = new FileWriter("output.txt");
        BufferedWriter w = new BufferedWriter(a);
        w.write("There are " + numPrimes + " primes smaller than " + k + "\r\n");
        for (int i=0; i<primes.size(); i++){
            w.write(primes.get(i).toString());
            w.write("\r\n");
        }
        w.flush();
        a.close();
    }

}
