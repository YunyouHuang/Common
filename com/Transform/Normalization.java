package com.Transform;

import com.Data.DigitalFeature;
import com.Data.ElectricProfile;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;

/**
 * Created by hdp on 16-10-9.
 */
public class Normalization {


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

    // 以自身的均值以及标准差规范化
    public static ArrayList<Double> zscore(ArrayList<Double> x){
        double mean=0;
        for (int i = 0; i < x.size(); i++) {
            mean+=x.get(i);
        }
        mean=mean/x.size();

        double var=0;
        for (int i = 0; i < x.size(); i++) {
            var+=Math.pow(x.get(i)-mean,2);
        }

        //var=var/(x.size()-1);
        var=var/x.size();
        double std=Math.sqrt(var);

        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < x.size(); i++) {
            if(std!=0){
                retValue.add((x.get(i)-mean)/std);
            }else{
                retValue.add(0.0);
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

        class getEleInstance implements Function<String,ElectricProfile>{
            @Override
            public ElectricProfile call(String s) throws Exception {
                return new ElectricProfile(s);
            }
        }

        DigitalFeature df=new DigitalFeature();

        JavaRDD<ElectricProfile> instanceData=lines.map(new getEleInstance());
        //instanceData.cache();
        //final ArrayList<Double> dataVar=df.variance(instanceData);
        //final ArrayList<Double> dataMean=df.getMeans(instanceData);

        //final int norFlag=Integer.parseInt("4");//归一化标志，0表示不做归一化，1表示以最大值做归一化，2表示以总和做归一化，3表示利用全体数据方差以及标准差做归一化，4表示以自己的方差以及标准差做归一化,5表示用最大值和最小值做归一化
        final int norFlag=Integer.parseInt(args[1]);

        class nor implements Function<ElectricProfile,ElectricProfile>{
            @Override
            public ElectricProfile call(ElectricProfile s) throws Exception {
                ElectricProfile ele=s;
                COMTrans ct=new COMTrans();
                ArrayList<Double> tmpDataPoints=new ArrayList<Double>();
                if (norFlag==1){
                    tmpDataPoints=ct.normalizing(1,ele.getPoints());
                }else if(norFlag==2){
                    tmpDataPoints=ct.normalizing(0,ele.getPoints());
                //}else if (norFlag==3){
                   // tmpDataPoints=ct.norByMeansAndVar(ele.getPoints(),dataMean,dataVar);
                }else if(norFlag==4){
                    tmpDataPoints=ct.zscore(ele.getPoints());
                }
                else if (norFlag==5){
                    tmpDataPoints=norByMaxAndMin(ele.getPoints());
                }
                ele.setPoints(tmpDataPoints);
                return ele;
            }
        }

        String tmpStr=inputPath.trim();
        tmpStr=tmpStr.substring(0,tmpStr.length()-1);
        HDFSOutputPath=tmpStr+"-Nor/transBy-"+norFlag+"/";
        instanceData.map(new nor()).saveAsTextFile(HDFSOutputPath);
    }
}
