package it.unipd.dei.bdc1718;

import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * Collection of utility input/output methods
 */
public class InputOutput {

  public static Vector strToVector(String str) {
    String[] tokens = str.split(" ");
    double[] data = new double[tokens.length];
    for (int i=0; i<tokens.length; i++) {
      data[i] = Double.parseDouble(tokens[i]);
    }
    return Vectors.dense(data);
  }

  public static JavaRDD<Vector> readVectors(JavaSparkContext sc, String path) {
    return sc.textFile(path).map(InputOutput::strToVector);
  }

  public static ArrayList<Vector> readVectorsSeq(String path) throws IOException {
    if (Files.isDirectory(Paths.get(path))) {
      throw new IllegalArgumentException("readVectorsSeq is meant to read a single file. To read this path (which is a directory) use the spark-based `readVectors`");
    }

    ArrayList<Vector> result = new ArrayList<>();
    Files.lines(Paths.get(path))
            .map(InputOutput::strToVector)
            .forEach(result::add);
    return result;
  }

}
