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
import java.util.ArrayList;

/**
 * Created by hdp on 17-3-12.
 */
public class DataTranForPortDatabase {

    public static String getDate (String dataDate){
        return dataDate.split(" ")[0].replace("\"","").replace("-","");
    }

    public static int getPointIndex(String dataDate){
        String[] timeStr=dataDate.split(" ")[1].replace("\"","").split(":");
        return Integer.parseInt(timeStr[0])*4+Integer.parseInt(timeStr[1])/15;
    }

    public static void trans1To96(JavaPairRDD<String,String> dataSet,String savePath){

        class merge implements Function<Tuple2<String,Iterable<String>>,ElectricProfile>{
            @Override
            public ElectricProfile call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                double[] dataArry=new double[96];
                for (int i = 0; i < dataArry.length; i++) {
                    dataArry[i]=-2;
                }
                for (String data:stringIterableTuple2._2
                     ) {
                    String[] tmpData=data.split(";",-1);
                    int index=getPointIndex(tmpData[1]);
                    double consume=-2;
                    if (tmpData[2].equals("")){
                        consume=-1;
                    }else {
                        consume=Double.parseDouble(tmpData[2]);
                    }
                    dataArry[index]=consume;
                }

                int isComplete=1;
                String nullDataFlag="";//0表示有数据,-2表示没有读取到数据

                ArrayList<Double> pointData=new ArrayList<Double>();
                for (int i = 0; i < dataArry.length; i++) {
                    pointData.add(dataArry[i]);
                    if (dataArry[i]==-2){
                        isComplete=0;
                        nullDataFlag+="-2";
                    } else if (dataArry[i]==-1){
                        isComplete=0;
                        nullDataFlag+="-1";
                    }else {
                        nullDataFlag+="0";
                    }
                }

                String[] tmpKey=stringIterableTuple2._1.split("-");
                ElectricProfile eleData=new ElectricProfile();
                eleData.setID(Long.parseLong(tmpKey[0]));
                eleData.setDataDate(tmpKey[1]);
                eleData.setDataType(2);
                eleData.setOrgNO(" Pecan Street Inc");
                eleData.setGetDate("2017-03-12");
                eleData.setDataPointFlag(isComplete);//原指数据的点数，现在用来指示数据的完整性
                eleData.setDataWholeFlag(nullDataFlag);
                eleData.setPoints(pointData);
                return eleData;
            }
        }

        dataSet.groupByKey().map(new merge()).saveAsTextFile(savePath);

    }

    public static void main(String[] args) throws IOException {

        String inputPath=args[0];
        String HDFSOutputPath=args[1];
        String statisticsPath=args[2];


        SparkConf conf=new SparkConf().setAppName("TranOneTo96");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);

        class split implements PairFunction<String,String,String>{
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] tmpStr=s.split(";",-1);
                return new Tuple2<String, String>(tmpStr[0]+"-"+getDate(tmpStr[1]),s);
            }
        }

        trans1To96(lines.mapToPair(new split()),HDFSOutputPath);

        JavaRDD<String> lines2 = sc.textFile(HDFSOutputPath);

        class split2 implements Function<String,ElectricProfile>{
            @Override
            public ElectricProfile call(String s) throws Exception {
                return new ElectricProfile(s);
            }
        }

        JavaRDD<ElectricProfile> dataSet=lines2.map(new split2());
        dataSet.cache();

        String outputStr="";
        for (int i = 2011; i <2018 ; i++) {
            outputStr+=StatisticsPecanStreetIncDatabase.statisticByYear(dataSet,String.valueOf(i));
        }

        HDFSOperate.writeToHdfs(statisticsPath, outputStr);
    }
}
