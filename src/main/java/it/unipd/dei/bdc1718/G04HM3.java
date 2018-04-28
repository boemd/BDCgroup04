package it.unipd.dei.bdc1718;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
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
        ArrayList<Vector> P = InputOutput.readVectorsSeq(path);

        int sz = P.size();
        System.out.println("sz: "+sz);
        ArrayList<Long> WP = new ArrayList<>();
        for(int i=0; i<P.size(); i++){
            WP.add(1L);
        }


        int k;
        int k1;
        Scanner in = new Scanner(System.in);
        System.out.println("Insert the value of k and k1, where k1 is bigger than k (if k is bigger than k1, then I'll" +
                " swap them)");
        System.out.println("Insert the value of k:");
        k = in.nextInt();
        System.out.println("Insert the value of k1:");
        k1 = in.nextInt();
        in.close();

        k = k % sz;
        k1 = k1 % sz;

        if(k > k1){
            System.out.println("Swap k and k1");
            int tmp = k1;
            k1 = k;
            k = tmp;
        }
        System.out.println("k:"+k);
        System.out.println("k1:"+k1);

        // ----------------------------------------------- FIRST POINT -----------------------------------------------

        Long t0,t1;
        t0=System.currentTimeMillis();
        kcenter(P,k);

        P = InputOutput.readVectorsSeq(path);
        t1=System.currentTimeMillis();
        System.out.println("Elapsed time for kcenter: "+(t1-t0)+" ms.");

        for(int i = 0; i < P.size(); i++){
            WP.add(1L);
        }

        // ---------------------------------------------- SECOND POINT -----------------------------------------------

        ArrayList<Vector> C = kmeansPP(P, WP, k);
        //printArrayList(C);
        P = InputOutput.readVectorsSeq(path);

        double obj = kmeansObj(P,C);
        P = InputOutput.readVectorsSeq(path);

        System.out.println("The value returned by kmeansObj is: " + obj);

        // ----------------------------------------------- THIRD POINT -----------------------------------------------

        ArrayList<Vector> X = kcenter(P, k1);
        P = InputOutput.readVectorsSeq(path);
        ArrayList<Long> WX = new ArrayList<>();
        for(int i = 0; i < X.size(); i++){
            WX.add(1L);
        }
        C = kmeansPP(X,WX,k);
        //printArrayList(C);
        obj = kmeansObj(P,C);
        System.out.println("The value returned by kmeansObj (coreset version) is: " + obj);
    }

    private static ArrayList<Vector> kcenter(ArrayList<Vector> P, int k) {
        if (k <= 0) {
            return null;
        }

        //ArrayList in which to put the centers
        ArrayList<Vector> S = new ArrayList<>();

        //choose a random point on P, remove it and put it in S
        Random rand = new Random();
        int n = rand.nextInt(P.size()); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);        // Set P - S


        //ArrayList in which to put all distances between P\S and S
        ArrayList<Double> minDistPS = new ArrayList<>();
        //Initialization of the List. It will be updated with complexity O(|P|) in the for loop
        for(int i = 0; i < P.size(); i++){
            minDistPS.add(Double.MAX_VALUE);
        }


        for (int i = 0; i < k-1; i++) {

            //at the beginning of each iteration, the iterator points at the beginning of the ArrayList P
            ListIterator<Vector> iterP = P.listIterator();

            //iterator on minDistPS
            ListIterator<Double> iterDist = minDistPS.listIterator();

            //Update minDistPS
            Vector s = S.get(0); //the last vector added to S
            while(iterP.hasNext()){                                     //for each element of P
                double tmp = Vectors.sqdist(iterP.next(), s);               //distance of the n-th element of P and the last element of S
                if (tmp < iterDist.next()){                                   //if the new distance is lower than the old distance
                    iterDist.set(tmp);                                      //the new distance replaces the old one
                }
            }

            //find the index of the point of P\S with higher distance from S
            int maxIndex = maxIdx(minDistPS);


            int psize = P.size();
            ListIterator itrP = P.listIterator();
            ListIterator itrS = S.listIterator();
            ListIterator distPS = minDistPS.listIterator();
            for(int h = 0; h < psize; h++){
                Vector v = (Vector) itrP.next();
                distPS.next();
                if(h == maxIndex){        // if v is the famigerato vettore that maximizes the minimum distance
                    itrP.remove();      //remove v form P (whose secret identity is P\S)
                    distPS.remove();      //remove v's evil brother from DIST
                    itrS.add(v);        //v moves in S // Since we haven't called yet itrS.next(), v is added to the
                    // position 0
                }

            }

            /*
            //add the point to S
            S.add(P.get(maxIndex));


            //Vector r = iterP.remove(maxIndex);
            //remove a node also from minDistPS
            minDistPS.remove(maxIndex);
            */
        }
        return S;
    }

    // WE ARE NOT USING THIS METHOD
    private static ArrayList<ArrayList<Vector>> Partition(ArrayList<Vector> P, ArrayList<Vector> S) {
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

    private static int argMin(Vector p, ArrayList<Vector> S) {
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

//    //it works, tested in other program
//    private static void printClustering(ArrayList<ArrayList<Vector>> C) {
//        ListIterator<ArrayList<Vector>> iterC = C.listIterator();
//        for (int i = 0; iterC.hasNext(); i++) {
//            System.out.println("");
//            System.out.println("");
//            System.out.println("");
//            System.out.println("Cluster " + i + ":");
//            printArrayList(iterC.next());
//        }
//    }
//
    //it also works
    private static void printArrayList(ArrayList<Vector> P) {
        ListIterator<Vector> iterP = P.listIterator();
        while (iterP.hasNext()) {
            System.out.println(Arrays.toString(iterP.next().toArray()));
        }
    }

    private static ArrayList<Vector> kmeansPP(ArrayList<Vector> P, ArrayList<Long> WP, int k) {
        if (k <= 1) {
            return null;
        }

        ArrayList<Vector> S = new ArrayList<>();
        //choose a random point on P, remove it and put it in S
        Random rand = new Random();
        int n = rand.nextInt(P.size()); // Arbitrary point c_1
        S.add(P.get(n));
        P.remove(n);        // Set P - S
        WP.remove(n);


        //ArrayList in which to put all distances between P\S and S
        ArrayList<Double> minDistPS = new ArrayList<>();
        //Initialization of the List. It will be updated with complexity O(|P|) in the for loop
        for(int i = 0; i < P.size(); i++){
            minDistPS.add(Double.MAX_VALUE);
        }

        for (int i = 0; i < k - 1; i++) {

            //at the beginning of each iteration, the iterator points at the beginning of the ArrayList P
            ListIterator<Vector> iterP = P.listIterator();

            //iterator on minDistPS
            ListIterator<Double> iterDist = minDistPS.listIterator();

            //Update minDistPS
            Vector s = S.get(0); //the last vector added to S
            while(iterP.hasNext()){                         //for each element of P
                double tmp = Vectors.sqdist(iterP.next(), s);               //distance of the n-th element of P and the last element of S
                if (tmp < iterDist.next()){                                   //if the new distance is lower than the old distance
                    iterDist.set(tmp);                                      //the new distance replaces the old one
                }
            }

            iterDist = minDistPS.listIterator();
            double sum=0;
            while(iterDist.hasNext()){
                sum+= Math.pow(iterDist.next(), 2);
            }
            iterDist = minDistPS.listIterator();
            double[] pi = new double[P.size()];

            for(int q=0; iterDist.hasNext(); q++){
                pi[q]=WP.get(q)*Math.pow(iterDist.next(), 2)/sum*WP.get(q);
            }


            // I draw a random number x that belongs to [0,1] to select the index of c_i as described in slide 15 of
            // "Clustering 2"
            double x = Math.random();
            double probSum = 0;
            int c_index = 0;
            for (int j = 0; j < P.size(); j++) { // Until the condition of x to be bigger than the sum of the probability
                probSum += pi[j];            // of selecting a certain p
                if (probSum > x) {
                    c_index = j;
                    break;
                }
            }

            int psize = P.size();
            ListIterator itrP = P.listIterator();
            ListIterator itrS = S.listIterator();
            ListIterator distPS = minDistPS.listIterator();
            for(int h = 0; h < psize; h++){
                Vector v = (Vector) itrP.next();
                distPS.next();
                if(h == c_index){        // if v is the famigerato vettore that maximizes the minimum distance
                    itrP.remove();      //remove v form P (whose secret identity is P\S)
                    distPS.remove();      //remove v's evil brother from DIST
                    WP.remove(c_index);
                    itrS.add(v);        //v moves in S // Since we haven't called yet itrS.next(), v is added to the
                    // position 0
                }

            }
        }
        return S;
    }

    private static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C){
        ArrayList<Double> distances = new ArrayList<>();
        ListIterator<Vector> iterP = P.listIterator();
        while (iterP.hasNext()){
            int indexP = iterP.nextIndex();
            int indexC = argMin(iterP.next(), C);
            double d = Vectors.sqdist(P.get(indexP),C.get(indexC));
            distances.add(d);
        }
        double sum = 0;
        for(int i = 0; i < distances.size(); i++)
        {
            sum = sum + distances.get(i);
        }

        return sum/distances.size();
    }

}
