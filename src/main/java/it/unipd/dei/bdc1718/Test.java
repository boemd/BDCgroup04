package it.unipd.dei.bdc1718;

import org.apache.avro.TestAnnotation;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.ArrayList;

import static it.unipd.dei.bdc1718.G04HM4.measure;

public class Test {

    public static void testMeasure(){

        ArrayList<Vector> v = new ArrayList<>();
        double d1[] = {0.1, 0.4, 0.7};
        double d2[] = {0.2, 0.5, 0.8};
        double d3[] = {0.3, 0.6, 0.9};
        v.add(Vectors.dense(d1));
        v.add(Vectors.dense(d2));
        v.add(Vectors.dense(d3));
        double m = measure(v);
        System.out.println(m);
    }

    public static void main(String[] args){
        testMeasure();
    }

}
