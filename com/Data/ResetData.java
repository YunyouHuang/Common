package com.Data;

import SparkClass.ReduceLong;
import SparkClass.SplitELE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by hyy on 2017/4/20.
 */
public class ResetData {

    public static HashMap<Long,BigDecimal>getFactors(String path) throws IOException {

        HashMap<Long,BigDecimal> retValue=new HashMap<Long, BigDecimal>();
        FileSystem fs = FileSystem.get(URI.create(path),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(path));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        for(int i = 0; i < fileList.length; i++) {
            if (!fileList[i].isDirectory()) {
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
                while ((line = in.readLine()) != null) {
                    String[] tmpStr = line.split("\t");
                    retValue.put(Long.parseLong(tmpStr[1]),new BigDecimal(tmpStr[2]));
                }
            }
        }
        if (in!=null){
            in.close();
        }
        if (fsi!=null){
            fsi.close();
        }
        return retValue;
    }

    public static void getUserLevel(String fPath,String dataInPath,String outPath) throws IOException {

        SparkConf conf=new SparkConf().setAppName("resetData");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(dataInPath).coalesce(2000);
        final HashMap<Long,BigDecimal> dataFactors=getFactors(fPath);
        class factor2user implements PairFunction<String,Double,Long>{
            @Override
            public Tuple2<Double, Long> call(String s) throws Exception {
                Tuple2<Double,Long> retValue;
                if (s.length()>150){
                    String[] tmpStr=s.split(" ",2);
                    long userId=Long.parseLong(tmpStr[0]);
                    double fac;
                    if (dataFactors.containsKey(userId)){
                        fac=dataFactors.get(userId).doubleValue();
                    }else {
                        fac=1;
                    }

                    retValue=new Tuple2<Double, Long>(fac,userId);
                }else {
                    retValue=new Tuple2<Double, Long>(-1.0,-1l);
                }
                return retValue;
            }
        }
        class trans2Num implements PairFunction<Tuple2<Double,Long>,Double,Long>{
            @Override
            public Tuple2<Double, Long> call(Tuple2<Double, Long> doubleLongTuple2) throws Exception {
                return new Tuple2<Double, Long>(doubleLongTuple2._1,1l);
            }
        }
        class trans2Str implements Function<Tuple2<Double,Long>,String>{
            @Override
            public String call(Tuple2<Double, Long> doubleLongTuple2) throws Exception {
                return doubleLongTuple2._1()+","+doubleLongTuple2._2;
            }
        }
        lines.mapToPair(new factor2user()).distinct().mapToPair(new trans2Num())
                .reduceByKey(new ReduceLong())
                .map(new trans2Str()).coalesce(1).saveAsTextFile(outPath);
    }
    public static void filterData(String fPath, String dataInPath, String outPath, final double maxFactor, double minFactor) throws IOException {

        SparkConf conf=new SparkConf().setAppName("resetData");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(dataInPath).coalesce(2000);
        final HashMap<Long,BigDecimal> dataFactors=getFactors(fPath);
        final double _maxFactor=maxFactor;
        final double _minFactor=minFactor;
        class fliterEle implements Function<String,Boolean>{
            @Override
            public Boolean call(String s) throws Exception {
                if (s.length()<150){
                    return false;
                }else {
                    String[] tmpStr=s.split(" ",2);
                    long userId=Long.parseLong(tmpStr[0]);
                    double fac;
                    if (dataFactors.containsKey(userId)){
                        fac=dataFactors.get(userId).doubleValue();
                    }else {
                        fac=1;
                    }

                    if ((fac<=_maxFactor)&&(fac>=_minFactor)){
                        return true;
                    }else {
                        return false;
                    }
                }
            }
        }
        lines.filter(new fliterEle()).coalesce(114).saveAsTextFile(outPath);
    }
    public static void recoverData(String fPath,String dataInPath,String outPath) throws IOException {
        SparkConf conf=new SparkConf().setAppName("resetData");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(dataInPath);
        JavaRDD<ElectricProfile> dataSet=lines.map(new SplitELE());
        final HashMap<Long,BigDecimal> dataFactors=getFactors(fPath);
        class mulFactors implements Function<ElectricProfile,ElectricProfile>{
            @Override
            public ElectricProfile call(ElectricProfile electricProfile) throws Exception {

                long userId=electricProfile.getID();
                if (dataFactors.containsKey(userId)){
                    BigDecimal factor=dataFactors.get(userId);
                    ArrayList<Double> tmpData=electricProfile.getPoints();
                    ArrayList<Double> retVule=new ArrayList<Double>();
                    for (int i = 0; i < tmpData.size(); i++) {
                        retVule.add(factor.multiply(new BigDecimal(String.valueOf(tmpData.get(i)))).doubleValue());
                    }
                    electricProfile.setPoints(retVule);
                }
                return electricProfile;
            }
        }

        dataSet.map(new mulFactors()).saveAsTextFile(outPath);

    }

    public static void main(String[] args) throws IOException {
        String fPath=args[0];
        String dataInput=args[1];
        String outPut=args[2];
        int flag=Integer.parseInt(args[3]);
        double _max=Double.parseDouble(args[4]);
        double _min=Double.parseDouble(args[5]);

        if (flag==0){
            getUserLevel(fPath,dataInput,outPut);
        }else if (flag==1){
            filterData(fPath,dataInput,outPut,_max,_min);
        }else {
            recoverData(fPath,dataInput,outPut);
        }
        //recoverData(fPath,dataInput,outPut);
        //getUserLevel(fPath,dataInput,outPut);
    }
}
