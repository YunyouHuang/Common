package com.Data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by hdp on 17-3-15.
 */
public class CombineWeekOrYear {

    public static void main(String[] args) throws IOException {
        String inputPath=args[0];
        String combineData=args[1];
        String hdfsOut=args[2];

        SparkConf conf=new SparkConf().setAppName("Combine");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);
        JavaRDD<String> lines2=sc.textFile(combineData);


        class splitY implements PairFunction<String,String,Integer>{
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                WeekAndYearInstance ins=new WeekAndYearInstance(tmpStr[1]);

                return new Tuple2<String,Integer>(ins.getID()+"-"+ins.getDate(),Integer.parseInt(tmpStr[0]));
            }
        }

        class splitEle implements PairFunction<String,String,ElectricProfile>{
            @Override
            public Tuple2<String, ElectricProfile> call(String s) throws Exception {
                ElectricProfile tmpEle=new ElectricProfile(s);
                return new Tuple2<String, ElectricProfile>(tmpEle.getID()+"-"+tmpEle.getDataDate(),tmpEle);
            }
        }

        class getdata implements PairFunction<Tuple2<String,Tuple2<Integer,ElectricProfile>>,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(Tuple2<String, Tuple2<Integer, ElectricProfile>> stringTuple2Tuple2) throws Exception {
                return stringTuple2Tuple2._2;
            }
        }

        lines.mapToPair(new splitY()).join(lines2.mapToPair(new splitEle())).mapToPair(new getdata()).saveAsTextFile(hdfsOut);

    }
}
