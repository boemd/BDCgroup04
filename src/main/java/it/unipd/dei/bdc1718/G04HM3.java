package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Scanner;

public class G04HM3 {
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }
        String path = args[0];

        // Setup Spark
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        ArrayList<Vector> P = InputOutput.readVectorsSeq(path);

        int sz = P.size();                      // Size of the ArrayList P
        System.out.println("Size of P: " + sz);

        int k;
        int k1;
        Scanner in = new Scanner(System.in);
        System.out.println("Insert the value of k and k1, where k must be smaller than k1, which respectively are the " +
                "number of centers computed by the Farthest-First Traversal algorithm, and the number of centers computed " +
                "with a weighted variant of the kmeans++ algorithm.");

        System.out.println("Insert the value of k1:");
        k1 = in.nextInt();
        while (k1 > sz) {
            System.out.println("The value of k1 inserted is bigger than P.size() (= " + sz + "). Please insert a new " +
                    "value lower than that:");
            k1 = in.nextInt();
        }
        System.out.println("Insert the value of k:");
        k = in.nextInt();
        while (k > sz || k >= k1) {
            if (k > sz) {
                System.out.println("The value of k inserted is bigger than P.size() (= " + sz + "). Please insert a new " +
                        "value lower than that:");
                k = in.nextInt();
            }
            else {
                System.out.println("The value of k inserted is bigger (or equal) than k1. Please insert a new value " +
                        "lower than that:");
                k = in.nextInt();
            }
        }
        in.close();

        // ----------------------------------------------- FIRST POINT -----------------------------------------------

        Long t0,t1;
        t0 = System.currentTimeMillis();                                        // Start
        ArrayList<Vector> S = kcenter(P,k);         // <--------- kcenter
        t1 = System.currentTimeMillis();
        System.out.println("Elapsed time for kcenter: " + (t1-t0) + " ms.");    // Running time

        // ---------------------------------------------- SECOND POINT -----------------------------------------------

        P = InputOutput.readVectorsSeq(path);                                   // We initialize P again since our method
        // modifies it
        ArrayList<Long> WP = new ArrayList<>();                                 // Set of weights for P
        for(int i = 0; i < P.size(); i++){                                      // All weights of P equal to 1
            WP.add(1L);
        }

        ArrayList<Vector> C = kmeansPP(P, WP, k);   // <--------- kmeansPP
        P = InputOutput.readVectorsSeq(path);
        double obj = kmeansObj(P,C);                // <--------- kmeansObj
        P = InputOutput.readVectorsSeq(path);
        System.out.println("The value returned by kmeansObj is: " + obj);       // Result of kmeansObj

        // ----------------------------------------------- THIRD POINT -----------------------------------------------

        ArrayList<Vector> X = kcenter(P, k1);       // <--------- kcenter
        P = InputOutput.readVectorsSeq(path);
        ArrayList<Long> WX = new ArrayList<>();
        for(int i = 0; i < X.size(); i++){      // All weights of X equal to 1
            WX.add(1L);
        }
        C = kmeansPP(X,WX,k);                       // <--------- kmeansPP
        P = InputOutput.readVectorsSeq(path);
        obj = kmeansObj(P,C);                       // <--------- kmeansObj
        System.out.println("The value returned by kmeansObj (coreset version) is: " + obj);
        // Result of kmeansObj for the coreset
        // version
    }

    // kcenter method:
    private static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k) {
        if (k <= 0) {
            return null;
        }

        // ArrayList in which to put the centers
        ArrayList<Vector> S = new ArrayList<>();

        // Choose a random point on P, remove it and put it in S
        int n = (int) (Math.random() * (P.size() - 1)); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);        // IMPORTANT: we call this P but we use this as the ArrayList P\S


        // ArrayList in which to put all distances between P\S and S
        ArrayList<Double> minDistPS = new ArrayList<>();
        // Initialization of the List, it will be updated with complexity O(|P|) in the "for" loop
        for(int i = 0; i < P.size(); i++){
            minDistPS.add(Double.MAX_VALUE);
        }


        for (int i = 0; i < k-1; i++) {

            // At the beginning of each iteration, the iterator points at the beginning of the ArrayList P
            ListIterator<Vector> iterP = P.listIterator();

            // Iterator on minDistPS
            ListIterator<Double> iterDist = minDistPS.listIterator();

            // Update minDistPS
            Vector s = S.get(0); //the last vector added to S
            while(iterP.hasNext()){                             // for each element of P
                double tmp = Vectors.sqdist(iterP.next(), s);   // distance of the n-th element of P and the last element of S
                if (tmp < iterDist.next()){                     // if the new distance is lower than the old distance
                    iterDist.set(tmp);                          // the new distance replaces the old one
                }
            }

            // Find the index of the point of P\S with higher distance from S
            int maxIndex = maxIdx(minDistPS);

            // This part is needed to add the vector from P to S
            int psize = P.size();
            ListIterator itrP = P.listIterator();
            ListIterator itrS = S.listIterator();
            ListIterator distPS = minDistPS.listIterator();
            for (int h = 0; h < psize; h++) {   // "for" loop to find the vector of P\S which is in the position maxIndex
                Vector v = (Vector) itrP.next();
                distPS.next();
                if (h == maxIndex){
                    itrP.remove();
                    distPS.remove();
                    itrS.add(v);                // Since we haven't called yet itrS.next(), v is added to the position 0
                }

            }
        }
        return S;
    }

    // Method to find the index of the point of P\S with higher distance from S; used in the kcenter method
    private static int maxIdx(ArrayList<Double> S) {
        int maxIdx = -1;
        double max = Double.MIN_VALUE;
        ListIterator<Double> iterS = S.listIterator();
        for (int i = 0; iterS.hasNext(); i++) {
            double tmp = iterS.next();
            if (tmp > max) {
                max = tmp;
                maxIdx = i;
            }
        }
        return maxIdx;
    }

    // kmeansPP method:
    private static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k) {
        if (k <= 1) {
            return null;
        }

        ArrayList<Vector> S = new ArrayList<>();
        // Choose a random point on P, remove it and put it in S
        int n = (int) (Math.random() * (P.size() - 1)); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);        // As in kcenter method, we use this variable as the set P\S
        WP.remove(n);


        // ArrayList in which to put all distances between P\S and S
        ArrayList<Double> minDistPS = new ArrayList<>();
        // Initialization of the List. It will be updated with complexity O(|P|) in the for loop
        for(int i = 0; i < P.size(); i++){
            minDistPS.add(Double.MAX_VALUE);
        }

        for (int i = 0; i < k - 1; i++) {

            // At the beginning of each iteration, the iterator points at the beginning of the ArrayList P
            ListIterator<Vector> iterP = P.listIterator();

            // Iterator on minDistPS
            ListIterator<Double> iterDist = minDistPS.listIterator();

            // Update minDistPS
            Vector s = S.get(0); // The last vector added to S
            while(iterP.hasNext()){                             //for each element of P
                double tmp = Vectors.sqdist(iterP.next(), s);   //distance of the n-th element of P and the last element of S
                if (tmp < iterDist.next()){                     //if the new distance is lower than the old distance
                    iterDist.set(tmp);                          //the new distance replaces the old one
                }
            }

            // This part is needed to obtain the probability to choose each element of P
            iterDist = minDistPS.listIterator();
            double sum=0;
            while(iterDist.hasNext()){
                sum+= Math.pow(iterDist.next(), 2);             // DA CONTROLLARE PER IL FATTO CHE sqdist RESTITUISCE
            }                                                   // UNA DISTANZA GIA' AL QUADRATO
            iterDist = minDistPS.listIterator();
            double[] pi = new double[P.size()];

            for(int q=0; iterDist.hasNext(); q++){
                pi[q]=WP.get(q)*Math.pow(iterDist.next(), 2)/sum*WP.get(q); // Variant probability for kmeans++
            }


            // I draw a random number x that belongs to [0,1] to select the index of c_i as described in slide 15 of
            // "Clustering 2"
            double x = Math.random();
            double probSum = 0;
            int c_index = 0;
            for (int j = 0; j < P.size(); j++) {    // Until the condition of x to be bigger than the sum of the probability
                probSum += pi[j];                   // of selecting a certain p
                if (probSum > x) {
                    c_index = j;
                    break;
                }
            }

            // "for" loop to find the vector of P\S which is in the position c_index
            int psize = P.size();
            ListIterator itrP = P.listIterator();
            ListIterator itrS = S.listIterator();
            ListIterator distPS = minDistPS.listIterator();
            for(int h = 0; h < psize; h++){
                Vector v = (Vector) itrP.next();
                distPS.next();
                if(h == c_index){
                    itrP.remove();
                    distPS.remove();
                    WP.remove(c_index);
                    itrS.add(v);        // Since we haven't called yet itrS.next(), v is added to the position 0
                }
            }
        }
        return S;
    }

    // kmeansObj method:
    private static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C){
        ArrayList<Double> distances = new ArrayList<>();
        ListIterator<Vector> iterP = P.listIterator();
        while (iterP.hasNext()){
            int indexP = iterP.nextIndex();
            int indexC = argMin(iterP.next(), C);   // Find the indexC of the element of C which have the minimum distance
            // from the actual element of P
            double d = Vectors.sqdist(P.get(indexP),C.get(indexC)); // Compute that distance
            distances.add(d);                       // Insert that distance in an ArrayList
        }
        double sum = 0;
        for(int i = 0; i < distances.size(); i++)
        {
            sum = sum + distances.get(i);           // Sum of every element of distances
        }

        return sum/distances.size();                // Average squared distance
    }

    // Method to find the index of the point of S with min distance from p; used in the kmeansObj method
    private static int argMin(Vector p, ArrayList<Vector> S) {
        int minIdx = -1;
        double minDistance = Double.MAX_VALUE;
        ListIterator<Vector> iterS = S.listIterator();
        for (int i = 0; iterS.hasNext(); i++) {
            double tmp = Vectors.sqdist(p, iterS.next());
            if (tmp < minDistance) {
                minDistance = tmp;
                minIdx = i;
            }
        }
        return minIdx;
    }
}