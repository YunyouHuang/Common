package com.Data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;

/**
 * Created by hdp on 17-7-13.
 */
public class NorShape {

    public static void main(String[] args) throws IOException {

        String input=args[0];
        String output=args[1];
        SparkConf conf=new SparkConf().setAppName("NorShape");//.setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);
        class splitEdge implements PairFunction<String,String,Double> {
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",");
                return new Tuple2<String,Double>(tmpStr[0]+"-"+tmpStr[1],Double.parseDouble(tmpStr[2]));
            }
        }

        class reduceForDistinct implements Function2<Double,Double,Double> {
            @Override
            public Double call(Double aFloat, Double aFloat2) throws Exception {
                double retValue=aFloat;
                if (Math.random()>0.5){
                    retValue=aFloat2;
                }
                return retValue;
            }
        }

        class trans implements Function<Tuple2<String,Double>,Tuple3<Long,Long,Double>> {
            @Override
            public Tuple3<Long, Long, Double> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                String[] tmpStr=stringDoubleTuple2._1.split("-");
                return new Tuple3<Long, Long, Double>(Long.parseLong(tmpStr[0]),Long.parseLong(tmpStr[1]),stringDoubleTuple2._2);
            }
        }
        JavaRDD<String> lines=sc.textFile(input).coalesce(250);

        JavaRDD<Tuple3<Long,Long,Double>> edgeSet=lines.mapToPair(new splitEdge()).reduceByKey(new reduceForDistinct()).map(new trans());
        edgeSet.saveAsTextFile(output);

    }
}
