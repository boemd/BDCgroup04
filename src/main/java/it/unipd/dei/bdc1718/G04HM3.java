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

public class G04HM3 {
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

    public static ArrayList<ArrayList<Vector>> kcenter(ArrayList<Vector> P, int k) {
        if (k <= 0) {
            return null;
        }

        ArrayList<Vector> S = new ArrayList<>();
        //choose a random point on P, remove it and put it in S
        int n = (int) (Math.random() * (P.size() - 1)); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);
        Vector current;
        double[] a, best, bestInit = {Double.MIN_VALUE, -1.0};

        for (int i = 0; i < k - 1; i++) {
            best = bestInit;
            ListIterator<Vector> iterP = P.listIterator();
            while (iterP.hasNext()) {
                current = iterP.next();
                a = distance(current, S);
                if (a[0] > best[0]) {
                    best = a;
                }
            }
            S.add(P.get((int) best[1]));
            P.remove((int) best[1]);
        }

        //ArrayList<ArrayList<Vector>> C = Partition(P, S);

        //return C;
        return Partition(P, S);        // Perchè non così? - Nick
    }

    public static ArrayList<ArrayList<Vector>> Partition(ArrayList<Vector> P, ArrayList<Vector> S) {
        //int this version, the intersection of P and S is empty
        ArrayList<ArrayList<Vector>> C = new ArrayList<>();
        ListIterator<ArrayList<Vector>> iterC = C.listIterator();
        //the first Vector of the list is the vector of centers
        C.add(S);
        ListIterator<Vector> iterS = S.listIterator();
        while (iterS.hasNext()) {
            iterC.next().add(iterS.next());
        }
        ListIterator<Vector> iterP = P.listIterator();
        Vector p;
        int index;
        while (iterP.hasNext()) {                        // Da riguardare pensando al commento 5 righe sopra, scritto così
            p = iterP.next();                             // mi sembra che scansioni tutto P, contando anche S - Nick
            index = argMin(p, S);
            C.get(index + 1).add(p);
        }

        return C;
    }

    public static double[] distance(Vector c, ArrayList<Vector> S) {
        double min = Double.MAX_VALUE;
        double current;
        int currIndex, minIndex = -1;
        ListIterator<Vector> iterS = S.listIterator();
        while (iterS.hasNext()) {
            currIndex = iterS.nextIndex();
            current = Vectors.sqdist(c, iterS.next());
            if (current < min) {
                min = current;
                minIndex = currIndex;
            }
        }
        double[] out = {min, minIndex};
        return out;
    }

    public static int argMin(Vector p, ArrayList<Vector> S) {
        int minIdx = -1;
        double minDistance = Double.MAX_VALUE;
        ListIterator<Vector> iterS = S.listIterator();
        for (int i = 0; iterS.hasNext(); i++) {
            if (Vectors.sqdist(p, iterS.next()) < minDistance) {
                minIdx = i;
            }
        }
        return minIdx;
    }

    static void printClustering(ArrayList<ArrayList<Vector>> C) {
        ListIterator<ArrayList<Vector>> iterC = C.listIterator();
        System.out.println("Centers");
        for (int i = 0; iterC.hasNext(); i++) {
            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("Cluster " + i + ":");
            printArrayList(iterC.next());
        }
    }

    static void printArrayList(ArrayList<Vector> P) {
        ListIterator<Vector> iterP = P.listIterator();
        while (iterP.hasNext()) {
            System.out.println(Arrays.toString(iterP.next().toArray()));
        }
    }

    public static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k) {
        if (k <= 1) {
            return null;
        }

        ArrayList<Vector> S = new ArrayList<>();
        ArrayList<Vector> PS = new ArrayList<>();       // ArrayList of P - S; messo per il ciclo for interno, da
        // modificare dopo aver risolto la parte iniziale del ciclo for
        // qua sotto
        //choose a random point on P, remove it and put it in S
        int n = (int) (Math.random() * (P.size() - 1)); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);
        Vector current;
        double[] a, best, bestInit = {Double.MIN_VALUE, -1.0};

        for (int i = 0; i < k - 1; i++) {
            // for() metodo per calcolare la distanza minima tra c e p appartenente all'insieme P - S (da rivedere il
            // metodo in Partition)
            // In questa parte calcolare anche un vettore di probabilità di "pescare" quel c_i
            // con questa probabilità:      w_p*(d_p)^2/(sum_{q non center} w_q*(d_q)^2)
            double[] pProb = null;    // Da inizializzare prima del ciclo for da fare del commento qua sopra

            // I draw a random number x that belongs to [0,1] to select the index of c_i as described in slide 15 of
            // "Clustering 2"
            double x = Math.random();
            double probSum = 0;
            int c_index = 0;
            for (int j = 0; j < PS.size(); j++) { // Until the condition of x to be bigger than the sum of the probability
                probSum += pProb[j];            // of selecting a certain p
                if (probSum > x) {
                    c_index = j;
                    break;
                }
            }
            S.add(PS.get(c_index));
            PS.remove(c_index);
        }
        return S;
    }

}
