package com.Data;

import com.FileOperate.HDFSOperate;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;

/**
 * Created by hdp on 17-3-13.
 */
public class dataSelectForPortData {

    public static void main(String[] args) throws IOException {

        String inputPath=args[0];
        String HDFSOutputPath=args[1];
        String statisticsPath=args[2];


        SparkConf conf=new SparkConf().setAppName("TranOneTo96");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines2 = sc.textFile(inputPath);

        class split2 implements Function<String,ElectricProfile> {
            @Override
            public ElectricProfile call(String s) throws Exception {
                return new ElectricProfile(s);
            }
        }

        class fliter implements Function<ElectricProfile,Boolean>{
            @Override
            public Boolean call(ElectricProfile electricProfile) throws Exception {
                int date=Integer.parseInt(electricProfile.getDataDate());
                return (electricProfile.getDataPointFlag()==1)&&(((date>=20141229)&&(date<20150302))||((date>20150308)&&(date<=20160103)));
            }
        }

        class transPair implements PairFunction<ElectricProfile,Long,ElectricProfile> {
            @Override
            public Tuple2<Long, ElectricProfile> call(ElectricProfile electricProfile) throws Exception {
                return new Tuple2<Long, ElectricProfile>(electricProfile.getID(),electricProfile);
            }
        }

        class countEle implements PairFunction<Tuple2<Long,Iterable<ElectricProfile>>,Long,Integer>{
            @Override
            public Tuple2<Long,Integer> call(Tuple2<Long, Iterable<ElectricProfile>> longIterableTuple2) throws Exception {
                int eleCount=0;
                for (ElectricProfile ele:longIterableTuple2._2
                        ) {
                    eleCount++;
                }
                return new Tuple2<Long,Integer>(longIterableTuple2._1,eleCount);
            }
        }

        class fliterNum implements Function<Tuple2<Long,Integer>,Boolean>{
            @Override
            public Boolean call(Tuple2<Long, Integer> longIntegerTuple2) throws Exception {
                return longIntegerTuple2._2==364;
            }
        }

        class getUser implements PairFunction<Tuple2<Long,Tuple2<ElectricProfile,Integer>>,SortdKey,ElectricProfile>{
            @Override
            public Tuple2<SortdKey, ElectricProfile> call(Tuple2<Long, Tuple2<ElectricProfile, Integer>> longTuple2Tuple2) throws Exception {
                return new Tuple2<SortdKey, ElectricProfile>(
                        new SortdKey(longTuple2Tuple2._1,Double.parseDouble(longTuple2Tuple2._2._1.getDataDate())),longTuple2Tuple2._2._1
                );
            }
        }

        class getEle implements Function<Tuple2<SortdKey,ElectricProfile>,ElectricProfile>{
            @Override
            public ElectricProfile call(Tuple2<SortdKey, ElectricProfile> sortdKeyElectricProfileTuple2) throws Exception {
                return sortdKeyElectricProfileTuple2._2;
            }
        }


        JavaRDD<ElectricProfile> dataSet=lines2.map(new split2());
        //dataSet.cache();
        JavaPairRDD<Long,ElectricProfile> dataSetWithID=dataSet.filter(new fliter()).mapToPair(new transPair());
        dataSetWithID.cache();

        dataSetWithID.join((dataSetWithID.groupByKey().mapToPair(new countEle()).filter(new fliterNum())))
                .mapToPair(new getUser()).sortByKey().map(new getEle()).saveAsTextFile(HDFSOutputPath);

        long count =(dataSetWithID.groupByKey().mapToPair(new countEle()).filter(new fliterNum())).count();
        String outputStr="total: "+count;

        HDFSOperate.writeToHdfs(statisticsPath, outputStr);
    }
}
