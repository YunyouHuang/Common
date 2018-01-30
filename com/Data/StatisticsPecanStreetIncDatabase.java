package com.Data;

import com.FileOperate.HDFSOperate;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

/**
 * Created by hdp on 17-3-12.
 */

public class StatisticsPecanStreetIncDatabase {

    public static String statisticByYear(JavaRDD<ElectricProfile> dataSet,String year){

        String retValue="**************\n**************\n"+year+"\n" +
                "**************\n" +
                "**************\n";
        final String years=year;

        class filter implements Function<ElectricProfile,Boolean>{
            @Override
            public Boolean call(ElectricProfile electricProfile) throws Exception {
                return electricProfile.getDataDate().contains(years);
            }
        }
        class filterWhole implements Function<ElectricProfile,Boolean>{
            @Override
            public Boolean call(ElectricProfile electricProfile) throws Exception {
                return electricProfile.getDataPointFlag()==1;
            }
        }

        class fliterNull implements Function<ElectricProfile,Boolean>{
            @Override
            public Boolean call(ElectricProfile electricProfile) throws Exception {
                return electricProfile.getDataWholeFlag().contains("-1");
            }
        }

        class transPair implements PairFunction<ElectricProfile,Long,ElectricProfile>{
            @Override
            public Tuple2<Long, ElectricProfile> call(ElectricProfile electricProfile) throws Exception {
                return new Tuple2<Long, ElectricProfile>(electricProfile.getID(),electricProfile);
            }
        }

        class countEle implements PairFunction<Tuple2<Long,Iterable<ElectricProfile>>,Integer,Long>{
            @Override
            public Tuple2<Integer,Long> call(Tuple2<Long, Iterable<ElectricProfile>> longIterableTuple2) throws Exception {
                int eleCount=0;
                for (ElectricProfile ele:longIterableTuple2._2
                     ) {
                    eleCount++;
                }
                return new Tuple2<Integer,Long>(eleCount,longIterableTuple2._1);
            }
        }

        class getdate implements PairFunction<ElectricProfile,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(ElectricProfile electricProfile) throws Exception {
                return new Tuple2<Integer, ElectricProfile>(Integer.parseInt(electricProfile.getDataDate()),electricProfile);
            }
        }

        class countForDate implements PairFunction<Tuple2<Integer,Iterable<ElectricProfile>>,Integer,Integer>{
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Iterable<ElectricProfile>> integerIterableTuple2) throws Exception {
                int eleCount=0;
                for (ElectricProfile ele:integerIterableTuple2._2
                        ) {
                    eleCount++;
                }
                return new Tuple2<Integer,Integer>(integerIterableTuple2._1,eleCount);
            }
        }


        JavaRDD<ElectricProfile> yearDataSet=dataSet.filter(new filter());
        yearDataSet.cache();
        long totalEle=yearDataSet.count();
        long totalNullEle=yearDataSet.filter(new fliterNull()).count();
        JavaRDD<ElectricProfile> wholeDataSet=yearDataSet.filter(new filterWhole());
        wholeDataSet.cache();
        long wholeEle=wholeDataSet.count();

        retValue+="totalEle----"+totalEle+"\n";
        retValue+="totalNullEle---"+totalNullEle+"\n";
        retValue+="wholeEle----"+wholeEle+"\n";
        retValue+="===========================\n";
        List<Tuple2<Integer,Long>> userWithCount=wholeDataSet.mapToPair(new transPair()).groupByKey().mapToPair(new countEle()).sortByKey().collect();
        for (Tuple2<Integer,Long> data:userWithCount
             ) {
            retValue+=data._2+"    "+data._1+"\n";
        }
        retValue+="===========================\n\n\n";
        retValue+="+++++++++++++++++++++++++++++\n";
        List<Tuple2<Integer,Integer>> yearCount=wholeDataSet.mapToPair(new getdate()).groupByKey().mapToPair(new countForDate()).sortByKey().collect();
        for (Tuple2<Integer,Integer> data:yearCount
             ) {
            retValue+=data._1+"    "+data._2+"\n";
        }
        retValue+="+++++++++++++++++++++++++++++\n\n\n";



        return retValue;
    }
    public static void main(String[] args) throws IOException {
        String inputPath=args[0];
        String HDFSOutputPath=args[1];


        SparkConf conf=new SparkConf().setAppName("Statistics");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);

        class split implements Function<String,ElectricProfile>{
            @Override
            public ElectricProfile call(String s) throws Exception {
                return new ElectricProfile(s);
            }
        }

        JavaRDD<ElectricProfile> dataSet=lines.map(new split());
        dataSet.cache();

        String outputStr="";
        for (int i = 2011; i <2018 ; i++) {
            outputStr+=statisticByYear(dataSet,String.valueOf(i));
        }

        HDFSOperate.writeToHdfs(HDFSOutputPath, outputStr);
    }
}
