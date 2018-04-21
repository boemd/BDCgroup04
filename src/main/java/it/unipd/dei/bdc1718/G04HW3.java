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
        printClustering(kcenter(a, 3));
    }

    public static ArrayList<ArrayList<Vector>> kcenter(ArrayList<Vector> P, int k){
        if (k == 0) {
            return null;
        }

        ArrayList<Vector> S = new ArrayList<Vector>();
        //choose a random point on P, remove it and put it in S
        int n = (int)(Math.random()*(P.size()-1)); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);
        Vector current;
        double[] a, best, bestInit = {Double.MIN_VALUE, -1.0};

        for(int i=0; i<k-1; i++){
            best = bestInit;
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
        }

        ArrayList<ArrayList<Vector>> C = Partition(P, S);

        return C;
    }

    public static ArrayList<ArrayList<Vector>> Partition(ArrayList<Vector> P, ArrayList<Vector> S){
        //int this version, the intersection of P and S is empty
        ArrayList<ArrayList<Vector>> C = new ArrayList<>();
        ListIterator<ArrayList<Vector>> iterC = C.listIterator();
        //the first Vector of the list is the vector of centers
        C.add(S);
        ListIterator<Vector> iterS = S.listIterator();
        while(iterS.hasNext()){
            iterC.next().add(iterS.next());
        }
        ListIterator<Vector> iterP = P.listIterator();
        Vector p;
        int index;
        while (iterP.hasNext()){
            p=iterP.next();
            index = argMin(p,S);
            C.get(index+1).add(p);
        }

        return C;
    }

    public static double[] distance(Vector c, ArrayList<Vector> S){
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

    public static int argMin(Vector p, ArrayList<Vector> S){
        int minIdx =-1;
        double minDistance = Double.MAX_VALUE;
        ListIterator<Vector> iterS = S.listIterator();
        for(int i=0; iterS.hasNext(); i++){
            if(Vectors.sqdist(p, iterS.next())<minDistance){
                minIdx = i;
            }
        }
        return minIdx;
    }

    static void printClustering(ArrayList<ArrayList<Vector>> C){
        ListIterator<ArrayList<Vector>> iterC = C.listIterator();
        System.out.println("Centers");
        for (int i=0; iterC.hasNext(); i++){
            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("Cluster "+i+":");
            printArrayList(iterC.next());
        }
    }

    static void printArrayList(ArrayList<Vector> P){
        ListIterator<Vector> iterP = P.listIterator();
        while(iterP.hasNext()){
            System.out.println(Arrays.toString(iterP.next().toArray()));
        }
    }
}