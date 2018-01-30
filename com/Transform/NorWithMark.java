package com.Transform;

import com.Data.DigitalFeature;
import com.Data.ElectricProfile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by hdp on 16-10-9.
 */
public class NorWithMark {


    public static double getMax(ArrayList<Double> points) {
        double retValue=0;
        for (int i = 0; i < points.size(); i++) {
            if (points.get(i)>retValue) {
                retValue=points.get(i);
            }
        }
        return retValue;
    }

    public static double getMin(ArrayList<Double> points) {
        double retValue=Double.MAX_VALUE;
        for (int i = 0; i < points.size(); i++) {
            if (points.get(i)<retValue) {
                retValue=points.get(i);
            }
        }
        return retValue;
    }

    //以最大值和最小值做归一化，x'=(x-xMin)/(xMax-xMin)
    public static ArrayList<Double> norByMaxAndMin(ArrayList<Double> x){
        double minValue=getMin(x);
        double maxValue=getMax(x);
        ArrayList<Double> retValue=new ArrayList<Double>();
        double norBase=maxValue-minValue;
        if (maxValue==0.0){
            retValue.addAll(x);
        }
        else{
            for (int i = 0; i <x.size(); i++) {
                retValue.add((x.get(i)-minValue)/norBase);
            }
        }
        return retValue;
    }

    //第一个参数为数据输入的路径，第二个参数为采取归一化的方式
    public static void main(String[] args){

        String inputPath=args[0];
        //String master="spark://10.30.5.137:7077";
        //String jPath="/home/hdp/IdeaProjects/ElectricityBigDataAnalysis/out/artifacts/ElectricityBigDataAnalysis_jar/ElectricityBigDataAnalysis.jar";
        //String inputPath="hdfs://10.30.5.137:9000/spark/originData/test10000/";
        String HDFSOutputPath="";
        //String[] jarPath={jPath};
        SparkConf conf=new SparkConf().setAppName("Nor");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);
        lines.cache();

        class getEleInstance implements PairFunction<String,Long,ElectricProfile> {
            @Override
            public Tuple2<Long,ElectricProfile> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                return new Tuple2<Long, ElectricProfile>(Long.parseLong(tmpStr[0]),new ElectricProfile(tmpStr[1]));
            }
        }

        DigitalFeature df=new DigitalFeature();

        class getEleRDD implements Function<Tuple2<Long,ElectricProfile>,ElectricProfile>{
            @Override
            public ElectricProfile call(Tuple2<Long, ElectricProfile> longElectricProfileTuple2) throws Exception {
                return longElectricProfileTuple2._2;
            }
        }
        JavaRDD<ElectricProfile> instanceData=lines.mapToPair(new getEleInstance()).map(new getEleRDD());
        instanceData.cache();
        final ArrayList<Double> dataVar=df.variance(instanceData);
        final ArrayList<Double> dataMean=df.getMeans(instanceData);
        instanceData.unpersist();
        //final int norFlag=Integer.parseInt("4");//归一化标志，0表示不做归一化，1表示以最大值做归一化，2表示以总和做归一化，3表示利用全体数据方差以及标准差做归一化，4表示以自己的方差以及标准差做归一化,5表示用最大值和最小值做归一化
        final int norFlag=Integer.parseInt(args[1]);

        class nor implements PairFunction<String,Long,ElectricProfile>{
            @Override
            public Tuple2<Long,ElectricProfile> call(String s) throws Exception {

                String[] tmpStr=s.substring(1,s.length()-1).split(",",2);
                ElectricProfile ele=new ElectricProfile(tmpStr[1]);
                COMTrans ct=new COMTrans();
                ArrayList<Double> tmpDataPoints=new ArrayList<Double>();
                if (norFlag==1){
                    tmpDataPoints=ct.normalizing(1,ele.getPoints());
                }else if(norFlag==2){
                    tmpDataPoints=ct.normalizing(0,ele.getPoints());
                }else if (norFlag==3){
                    tmpDataPoints=ct.norByMeansAndVar(ele.getPoints(),dataMean,dataVar);
                }else if(norFlag==4){
                    tmpDataPoints=ct.zscore(ele.getPoints());
                }
                else if (norFlag==5){
                    tmpDataPoints=norByMaxAndMin(ele.getPoints());
                }
                ele.setPoints(tmpDataPoints);
                return new Tuple2<Long, ElectricProfile>(Long.parseLong(tmpStr[0]),ele);
            }
        }

        String tmpStr=inputPath.trim();
        tmpStr=tmpStr.substring(0,tmpStr.length()-1);
        HDFSOutputPath=tmpStr+"-Nor/transBy-"+norFlag+"/";

        lines.mapToPair(new nor()).saveAsTextFile(HDFSOutputPath);
    }
}
