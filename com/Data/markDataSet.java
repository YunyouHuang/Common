package com.Data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by hdp on 16-11-1.
 */
public class markDataSet {

    public static void main(String[] args) throws IOException{
        //String inputPath=args[0];
        //String outputPath=args[1];
        String master="spark://10.30.5.137:7077";
        String jarPath="/home/hdp/IdeaProjects/ElectricityBigDataAnalysis/out/artifacts/ElectricityBigDataAnalysis_jar/ElectricityBigDataAnalysis.jar";
        String inputPath="hdfs://10.30.5.137:9000/originData/";
        String[] jarPathArray={jarPath};
        //SparkConf conf=new SparkConf().setAppName("Encoding").setMaster(master).setJars(jarPath);

       // String inputPath=args[0];
        //int flag=Integer.parseInt(args[1]);
       // int blockSize=Integer.parseInt(args[2]);

        SparkConf conf=new SparkConf().setAppName("marking").setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);
        class transData implements PairFunction<Tuple2<String,Long>,Long,String>{
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return new Tuple2<Long, String>(stringLongTuple2._2,stringLongTuple2._1);
            }
        }
        lines.zipWithUniqueId().mapToPair(new transData()).saveAsTextFile("hdfs://10.30.5.137:9000/markedData/");
    }
}
