package com.Data;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by hdp on 17-2-26.
 */
public class getWeekAndYearData implements Serializable {

    public static void main(String[] args){
        String dataSetStr=args[0];
        String savePath=args[1];
        int flag=Integer.parseInt(args[2]);

        class getEleDate implements PairFunction<String,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(String s) throws Exception {

                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                int mark=Integer.parseInt(tmpStr[0]);
                return new Tuple2<Integer, ElectricProfile>(mark,new ElectricProfile(tmpStr[1].substring(1,tmpStr[1].length()-1)));
            }
        }

        class getWeekDate implements PairFunction<String,Integer,WeekAndYearInstance>{
            @Override
            public Tuple2<Integer, WeekAndYearInstance> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                int mark=Integer.parseInt(tmpStr[0]);
                return new Tuple2<Integer, WeekAndYearInstance>(mark,new WeekAndYearInstance(tmpStr[1].substring(1,tmpStr[1].length()-1)));
            }
        }
        SparkConf conf=new SparkConf().setAppName("dataClearGetDataSet");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(dataSetStr);
        DataClear dc=new DataClear();
        if (flag==0){
            //dc.getWeekDataSet(lines.mapToPair(new getEleDate()),savePath);
        }else {
           // dc.getYearDataSet(lines.mapToPair(new getWeekDate()),savePath);
        }
    }
}
