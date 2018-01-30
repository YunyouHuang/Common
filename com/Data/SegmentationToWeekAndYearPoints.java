package com.Data;

import com.Sort.InsertSort;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hdp on 17-3-15.
 */
public class SegmentationToWeekAndYearPoints {

    public static void getWeekDataSet(JavaPairRDD<Integer,ElectricProfile> dataSet, String Path){

        class getSortKey implements PairFunction<Tuple2<Integer,ElectricProfile>,SortdKey,Tuple2<Integer,ElectricProfile>> {
            @Override
            public Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                return new Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>>(
                        new SortdKey(integerElectricProfileTuple2._2.getID(),Double.parseDouble(integerElectricProfileTuple2._2.getDataDate())),integerElectricProfileTuple2
                );
            }
        }

        class getDataWithID implements PairFunction<Tuple2<SortdKey,Tuple2<Integer,ElectricProfile>>,Long,Tuple2<Integer,ElectricProfile>>{
            @Override
            public Tuple2<Long, Tuple2<Integer, ElectricProfile>> call(Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>> sortdKeyTuple2Tuple2) throws Exception {
                return new Tuple2<Long, Tuple2<Integer, ElectricProfile>>(sortdKeyTuple2Tuple2._1.getFirst(),sortdKeyTuple2Tuple2._2);
            }
        }

        class getWDataPoints implements FlatMapFunction<Tuple2<Long,Iterable<Tuple2<Integer,ElectricProfile>>>,ElectricProfile> {
            @Override
            public Iterable<ElectricProfile> call(Tuple2<Long, Iterable<Tuple2<Integer, ElectricProfile>>> longIterableTuple2) throws Exception {
                List<ElectricProfile> retValue=new ArrayList<ElectricProfile>();
                int count=0;
                String dataStr="";
                for (Tuple2<Integer,ElectricProfile> data: InsertSort.sortForEleWithMarkAscend(longIterableTuple2._2)
                        ) {
                    if (count==0){
                        dataStr+=data._2.getID()+","+data._2.getDataDate()+","+data._2.getDataType()+","+data._2.getOrgNO()
                                +","+data._2.getGetDate()+","+data._2.getDataPointFlag()+","+data._2.getDataWholeFlag();
                    }
                    for (int i = 0; i < data._2.getPoints().size(); i++) {
                        dataStr+=","+data._2.getPoints().get(i);
                    }

                    count++;
                    if (count>6){
                        count=0;
                        retValue.add(new ElectricProfile(dataStr));
                        dataStr="";
                    }
                }
                return retValue;
            }
        }
        dataSet.mapToPair(new getSortKey()).sortByKey().mapToPair(new getDataWithID()).groupByKey().flatMap(new getWDataPoints()).saveAsTextFile(Path);
    }

    public static void getYearDataSet(JavaPairRDD<Integer,ElectricProfile> dataSet, String Path){

        class getSortKey implements PairFunction<Tuple2<Integer,ElectricProfile>,SortdKey,Tuple2<Integer,ElectricProfile>> {
            @Override
            public Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                return new Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>>(
                        new SortdKey(integerElectricProfileTuple2._2.getID(),Double.parseDouble(integerElectricProfileTuple2._2.getDataDate())),integerElectricProfileTuple2
                );
            }
        }

        class getDataWithID implements PairFunction<Tuple2<SortdKey,Tuple2<Integer,ElectricProfile>>,Long,Tuple2<Integer,ElectricProfile>>{
            @Override
            public Tuple2<Long, Tuple2<Integer, ElectricProfile>> call(Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>> sortdKeyTuple2Tuple2) throws Exception {
                return new Tuple2<Long, Tuple2<Integer, ElectricProfile>>(sortdKeyTuple2Tuple2._1.getFirst(),sortdKeyTuple2Tuple2._2);
            }
        }

        class getWDataPoints implements Function<Tuple2<Long,Iterable<Tuple2<Integer,ElectricProfile>>>,ElectricProfile> {
            @Override
            public ElectricProfile call(Tuple2<Long, Iterable<Tuple2<Integer, ElectricProfile>>> longIterableTuple2) throws Exception {
                int count=0;
                String dataStr="";
                for (Tuple2<Integer,ElectricProfile> data:InsertSort.sortForEleWithMarkAscend(longIterableTuple2._2)
                        ) {
                    if (count==0){
                        dataStr+=data._2.getID()+","+data._2.getDataDate()+","+data._2.getDataType()+","+data._2.getOrgNO()
                                +","+data._2.getGetDate()+","+data._2.getDataPointFlag()+","+data._2.getDataWholeFlag();
                    }
                    for (int i = 0; i < data._2.getPoints().size(); i++) {
                        dataStr+=","+data._2.getPoints().get(i);
                    }
                    count++;
                }
                return new ElectricProfile(dataStr);
            }
        }

        dataSet.mapToPair(new getSortKey()).sortByKey().mapToPair(new getDataWithID()).groupByKey().map(new getWDataPoints()).saveAsTextFile(Path);
    }


    public static void main(String[] args) throws IOException {
        String inputPath=args[0];
        String HDFSOutputPath=args[1];
        int flag=Integer.parseInt(args[2]);

        SparkConf conf=new SparkConf().setAppName("Seg");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);

        class splitW implements PairFunction<String,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                return new Tuple2<Integer, ElectricProfile>(
                        Integer.parseInt(tmpStr[0]),
                        new ElectricProfile(tmpStr[1])
                );
            }
        }


        if (flag==0){
            getWeekDataSet(lines.mapToPair(new splitW()),HDFSOutputPath);
        }else {
            getYearDataSet(lines.mapToPair(new splitW()),HDFSOutputPath);
        }
    }
}
