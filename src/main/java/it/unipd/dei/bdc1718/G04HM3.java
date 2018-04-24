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
        System.out.println("GO: Preparation");

        // Setup Spark
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);
        ArrayList<Vector> A = InputOutput.readVectorsSeq(path);
        printArrayList(A);

        System.out.println("OK: Preparation");
        System.out.println("GO: K-center");
        ArrayList<Vector> S = kcenter(A,3);
        System.out.println("OK: K-center");
        System.out.println("GO: Partition");
        ArrayList<ArrayList<Vector>> C = Partition(A, S);
        System.out.println("OK: Partition");
        System.out.println("GO: Print clustering");
        printClustering(C);
        System.out.println("OK: Print clustering");
    }

    public static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k) {
        if (k <= 0) {
            return null;
        }

        //ArrayList in which to put the centers
        ArrayList<Vector> S = new ArrayList<>();

        //choose a random point on P, remove it and put it in S
        int n = (int) (Math.random() * (P.size() - 1)); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);
        System.out.println("    OK: random choice");


        //ArrayList in which to put all distances between P\S and S
        ArrayList<Double> minDistPS = new ArrayList<>();
        ListIterator<Vector> iterP = P.listIterator();
        //Initialization of the List. It will be updated with complexity O(|P|) in the for loop
        for(int i=0; i<P.size(); i++){
            minDistPS.add(Double.MAX_VALUE);
        }
        System.out.println("    OK: distances initialization");


        System.out.println("    GO: for-loop");
        for (int i = 0; i < k-1; i++) {

            //at the beginning of each iteration, the iterator points at the beginning of the ArrayList P
            iterP = P.listIterator();

            //iterator on minDistPS
            ListIterator<Double> iterDist =minDistPS.listIterator();

            //Update minDistPS
            Vector s = S.get(i); //the last vector added to S
            for(int l=0; iterP.hasNext(); l++){                         //for each element of P
                double tmp = Vectors.sqdist(iterP.next(), s);               //distance of the l-th element of P and the last element of S
                if (tmp<iterDist.next()){                                   //if the new distance is lower than the old distance
                    minDistPS.remove(l);                                        //the old distance gets removed
                    minDistPS.add(l, tmp);                                      //the new distance replaces the old distance
                }
            }
            System.out.println("        OK: update distance");

            //find the index of the point of P\S with higher distance from S
            int maxIndex = minDistPS.indexOf(Collections.max(minDistPS));
            System.out.println("        OK: maxIndex");

            //add the point to S
            S.add(P.get(maxIndex));
            //remove it form P (whose true identity is P\S)
            P.remove(maxIndex);
            //remove a node also from minDistPS
            minDistPS.remove(maxIndex);
            System.out.println("        OK: addition and removals");
        }

        return S;
    }

    public static ArrayList<ArrayList<Vector>> Partition(ArrayList<Vector> P, ArrayList<Vector> S) {
        System.out.println("GO: Partition");
        // I need P\S
        P.removeAll(S);
        System.out.println("    OK: removeall");

        //ArrayList in which I put the clusters
        ArrayList<ArrayList<Vector>> C = new ArrayList<>();
        ListIterator<ArrayList<Vector>> iterC = C.listIterator();

        //add each center to a list
        ListIterator<Vector> iterS = S.listIterator();
        while (iterS.hasNext()) {
            iterC.next().add(iterS.next());
        }
        System.out.println("    OK: initialization");

        ListIterator<Vector> iterP = P.listIterator();
        Vector p;
        int index;
        while (iterP.hasNext()) {       //for each p in P\S:
            p = iterP.next();
            index = argMin(p, S);           //compute the index of the list whose distance from p is minimum
            C.get(index).add(p);            //add p in that list
        }
        System.out.println("    OK: while-loop");


        return C;
    }

    //DO WE REALLY NEED IT?
    /*
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
    */

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

    //it works, tested in other program
    static void printClustering(ArrayList<ArrayList<Vector>> C) {
        ListIterator<ArrayList<Vector>> iterC = C.listIterator();
        for (int i = 0; iterC.hasNext(); i++) {
            System.out.println("");
            System.out.println("");
            System.out.println("");
            System.out.println("Cluster " + i + ":");
            printArrayList(iterC.next());
        }
    }

    //it also works
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
