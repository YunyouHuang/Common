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
 * Created by hdp on 17-3-14.
 */
public class Segmentation {

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

        class getWData implements FlatMapFunction<Tuple2<Long,Iterable<Tuple2<Integer,ElectricProfile>>>,WeekAndYearInstance> {
            @Override
            public Iterable<WeekAndYearInstance> call(Tuple2<Long, Iterable<Tuple2<Integer, ElectricProfile>>> longIterableTuple2) throws Exception {
                List<WeekAndYearInstance> retVal=new ArrayList<WeekAndYearInstance>();
                String dataStr="";
                int count=0;
                for (Tuple2<Integer,ElectricProfile> data: InsertSort.sortForEleWithMarkAscend(longIterableTuple2._2)
                        ) {
                    if (count==0){
                        dataStr+=data._2.getID()+","+data._2.getDataDate();
                    }
                    dataStr+=","+data._1;
                    count++;
                    if (count>6){
                        count=0;
                        retVal.add(new WeekAndYearInstance(dataStr));
                        dataStr="";
                    }
                }
                return retVal;
            }
        }

        dataSet.mapToPair(new getSortKey()).sortByKey().mapToPair(new getDataWithID()).groupByKey().flatMap(new getWData()).saveAsTextFile(Path);
    }

    public static void getYearDataSet(JavaPairRDD<Integer,WeekAndYearInstance> dataSet,String Path){

        class getYearSortKey implements PairFunction<Tuple2<Integer,WeekAndYearInstance>,SortdKey,Tuple2<Integer,WeekAndYearInstance>>{
            @Override
            public Tuple2<SortdKey, Tuple2<Integer, WeekAndYearInstance>> call(Tuple2<Integer, WeekAndYearInstance> integerWeekAndYearInstanceTuple2) throws Exception {
                return new Tuple2<SortdKey, Tuple2<Integer, WeekAndYearInstance>>(
                        new SortdKey(integerWeekAndYearInstanceTuple2._2.getID(),Double.parseDouble(integerWeekAndYearInstanceTuple2._2.getDate())),
                        integerWeekAndYearInstanceTuple2
                );
            }
        }

        class getDataWithUserID implements PairFunction<Tuple2<SortdKey,Tuple2<Integer,WeekAndYearInstance>>,Long,Tuple2<Integer,WeekAndYearInstance>>{
            @Override
            public Tuple2<Long, Tuple2<Integer, WeekAndYearInstance>> call(Tuple2<SortdKey, Tuple2<Integer, WeekAndYearInstance>> sortdKeyTuple2Tuple2) throws Exception {
                return new Tuple2<Long, Tuple2<Integer, WeekAndYearInstance>>(sortdKeyTuple2Tuple2._1.getFirst(),sortdKeyTuple2Tuple2._2);
            }
        }

        class  getYearData implements Function<Tuple2<Long,Iterable<Tuple2<Integer,WeekAndYearInstance>>>,WeekAndYearInstance> {

            @Override
            public WeekAndYearInstance call(Tuple2<Long, Iterable<Tuple2<Integer, WeekAndYearInstance>>> longIterableTuple2) throws Exception {
                String dataStr="";
                int count=0;
                for (Tuple2<Integer,WeekAndYearInstance> data:InsertSort.sortForWYWithMarkAscend(longIterableTuple2._2)
                        ) {
                    if (count==0){
                        dataStr+=data._2.getID()+","+data._2.getDate();
                    }
                    dataStr+=","+data._1;
                    count++;
                }
                return new WeekAndYearInstance(dataStr);
            }
        }

        dataSet.mapToPair(new getYearSortKey()).sortByKey().mapToPair(new getDataWithUserID()).groupByKey().map(new getYearData()).saveAsTextFile(Path);
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

        class splitY implements PairFunction<String,Integer,WeekAndYearInstance>{
            @Override
            public Tuple2<Integer, WeekAndYearInstance> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                return new Tuple2<Integer, WeekAndYearInstance>(
                        Integer.parseInt(tmpStr[0]),
                        new WeekAndYearInstance(tmpStr[1])
                );
            }
        }

        if (flag==0){
            getWeekDataSet(lines.mapToPair(new splitW()),HDFSOutputPath);
        }else {
            getYearDataSet(lines.mapToPair(new splitY()),HDFSOutputPath);
        }
    }
}
