package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import it.unipd.dei.bdc1718.InputOutput;

import java.io.IOException;
import java.util.*;

public class G04HW3 {
    public static void main(String[] args) throws IOException {
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
        ArrayList<Vector> a = InputOutput.readVectorsSeq(path);
        double l = Vectors.sqdist(a.get(0),a.get(1));
        System.out.println(l);
    }

    public ArrayList<Vector> Partition(ArrayList<Vector> P, ArrayList<Vector> S){
        return null;
    }

    public ArrayList<Vector> kcenter(ArrayList<Vector> P, int k){
        // aggiungere condizione del cazzo k>1
        ArrayList<Vector> S = new ArrayList<Vector>();

        int n = (int)(Math.random()*(P.size()-1)); // Arbitrary point c_1

        S.add(P.get(n));
        P.remove(n);
        Vector current;
        double[] a, best, bestInit = {Double.MIN_VALUE, -1.0};
        best=bestInit;

        for(int i=0; i<k-1; i++){
            ListIterator<Vector> iterP = P.listIterator();
            for (int j=0; j<P.size(); j++){
                current = iterP.next();
                a = distance(current, S);
                if (a[0] > best[0]) {
                    best = a;
                }
            }
            S.add(P.get((int)best[1]));
            P.remove((int)best[1]);
            best = bestInit;
        }


        return null; // return S;
    }

    public double[] distance(Vector c, ArrayList<Vector> S){
        double min = Double.MAX_VALUE;
        double current;
        int currIndex, minIndex = -1;
        ListIterator<Vector> iterS = S.listIterator();
        for(int i = 0; i<S.size(); i++){
            currIndex = iterS.nextIndex();
            current = Vectors.sqdist(c, iterS.next());
            if (current < min){
                min = current;
                minIndex = currIndex;
            }
        }
        double[] out = {min, minIndex};
        return out;
    }
}