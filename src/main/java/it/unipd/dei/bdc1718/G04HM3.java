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
        ArrayList<Vector> A = InputOutput.readVectorsSeq(path);
        printArrayList(A);
        ArrayList<Vector> S = kcenter(A,3);
        System.out.println(S.size());
        printArrayList(S);

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


        //ArrayList in which to put all distances between P\S and S
        ArrayList<Double> minDistPS = new ArrayList<>();
        //Initialization of the List. It will be updated with complexity O(|P|) in the for loop
        for(int i=0; i<P.size(); i++){
            minDistPS.add(Double.MAX_VALUE);
        }


        for (int i = 0; i < k-1; i++) {

            //at the beginning of each iteration, the iterator points at the beginning of the ArrayList P
            ListIterator<Vector> iterP = P.listIterator();

            //iterator on minDistPS
            ListIterator<Double> iterDist =minDistPS.listIterator();

            //Update minDistPS
            Vector s = S.get(0); //the last vector added to S
            for(int l=0; iterP.hasNext(); l++){                         //for each element of P
                double tmp = Vectors.sqdist(iterP.next(), s);               //distance of the l-th element of P and the last element of S
                if (tmp<iterDist.next()){                                   //if the new distance is lower than the old distance
                        iterDist.set(tmp);                                      //the new distance replaces the old one
                }
            }

            //find the index of the point of P\S with higher distance from S
            int maxIndex = maxIdx(minDistPS);


            int psize = P.size();
            ListIterator itrP = P.listIterator();
            ListIterator itrS = S.listIterator();
            ListIterator distPS = minDistPS.listIterator();
            for(int h=0; h<psize; h++){
                Vector v = (Vector) itrP.next();
                distPS.next();
                if(h==maxIndex){        // if v is the famigerato vettore that maximizes the minimum distance
                    itrP.remove();      //remove v form P (whose secret identity is P\S)
                    distPS.remove();      //remove v's evil brother from S
                    itrS.add(v);        //v moves in S
                }

            }

            /*
            //add the point to S
            S.add(P.get(maxIndex));


            //Vector r = iterP.remove(maxIndex);
            //remove a node also from minDistPS
            minDistPS.remove(maxIndex);
            */
            printArrayList(S);
        }
        return S;
    }

    public static ArrayList<ArrayList<Vector>> Partition(ArrayList<Vector> P, ArrayList<Vector> S) {
        // I need P\S
        P.removeAll(S);

        //ArrayList in which I put the clusters
        ArrayList<ArrayList<Vector>> C = new ArrayList<>();
        ListIterator<ArrayList<Vector>> iterC = C.listIterator();

        //add each center to a list
        ListIterator<Vector> iterS = S.listIterator();
        while (iterS.hasNext()) {
            iterC.next().add(iterS.next());
        }

        ListIterator<Vector> iterP = P.listIterator();
        Vector p;
        int index;
        while (iterP.hasNext()) {       //for each p in P\S:
            p = iterP.next();
            index = argMin(p, S);           //compute the index of the list whose distance from p is minimum
            C.get(index).add(p);            //add p in that list
        }


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
            double tmp=Vectors.sqdist(p, iterS.next());
            if (tmp < minDistance) {
                minDistance=tmp;
                minIdx = i;
            }
        }
        return minIdx;
    }

    public static int maxIdx(ArrayList<Double> S) {
        int maxIdx = -1;
        double max = Double.MIN_VALUE;
        ListIterator<Double> iterS = S.listIterator();
        for (int i = 0; iterS.hasNext(); i++) {
            double tmp=iterS.next();
            if (tmp > max) {
                max=tmp;
                maxIdx = i;
            }
        }
        return maxIdx;
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

    public static ArrayList<Double> kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C){
        ArrayList<Double> distances = new ArrayList<>();
        ListIterator<Vector> iterP = P.listIterator();
        while (iterP.hasNext()){
            int indexP = iterP.nextIndex();
            int indexC = argMin(iterP.next(), C);
            double d = Vectors.sqdist(P.get(indexP),C.get(indexC));
            distances.add(d);
        }
        return distances;
    }

}
