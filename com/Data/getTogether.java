package com.Data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Created by hdp on 17-3-3.
 */
public class getTogether {
    public static void main(String[] args) throws IOException {

        String in=args[0];
        String out=args[1];

        SparkConf conf=new SparkConf().setAppName("dataClear");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
         sc.textFile(in).coalesce(96).saveAsTextFile(out);
    }
}
