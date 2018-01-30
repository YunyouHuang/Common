package com.Data;

import SparkClass.ReduceDouble;
import SparkClass.ReduceInteger;
import com.FileOperate.HDFSOperate;
import com.Similarity.DTWDistanForScalar;
import com.Similarity.Distance;
import com.Transform.FftConv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *数据集变量数字特征
 * Created by hdp on 16-9-10.
 */
public class DigitalFeature implements Serializable{


    //public int objectFlag=0;//用于表示当前的聚类对象是周还是年，flag=0表示周，flag=1表示年
    //public Matrix pcDistMatrix=Matrix.Factory.zeros(1,1);//用于保存日模式之间的距离
    //public Matrix wcDistMatrix=Matrix.Factory.zeros(1,1);//用于保存周模式之间的距离

    public FloatMatrix pcDistMatrix=null;//用于保存日模式之间的距离
    public FloatMatrix wcDistMatrix=null;//用于保存周模式之间的距离
    public int pcSize=-1;
    public int wcSize=-1;


    //获取整个数据集每一个时间点的平均值
    public static ArrayList<Double> getMeans(JavaRDD<ElectricProfile> dataSet){

        final Long count=dataSet.count();
        class getSumForAll implements Function2<ElectricProfile,ElectricProfile,ElectricProfile>{
            @Override
            public ElectricProfile call(ElectricProfile electricProfile, ElectricProfile electricProfile2) throws Exception {
                DataOperation dataOPer=new DataOperation();
                ArrayList<Double> sumArray=dataOPer.arrayPlu(electricProfile.getPoints(),electricProfile2.getPoints());
                ElectricProfile el=new ElectricProfile();
                el.setPoints(sumArray);
                return el;
            }
        }

        ElectricProfile sumEl=dataSet.reduce(new getSumForAll());
        DataOperation dataOper=new DataOperation();
        ArrayList<Double> means=dataOper.ArrayDivid(sumEl.getPoints(),count);
        return means;
    }

    //获取整个数据集每一个时间点的方差
    public static ArrayList<Double> variance(JavaRDD<ElectricProfile> dataSet){
        final Long count=dataSet.count();
        final ArrayList<Double> means=getMeans(dataSet);
        class getpOWForAll implements Function<ElectricProfile,ArrayList<Double>>{
            @Override
            public ArrayList<Double> call(ElectricProfile electricProfile) throws Exception {
                DataOperation dataOPer=new DataOperation();
                return dataOPer.arrayPow(dataOPer.arrayMinus(electricProfile.getPoints(),means));
            }
        }
        class reduceArray implements Function2<ArrayList<Double>,ArrayList<Double>,ArrayList<Double>>{
            @Override
            public ArrayList<Double> call(ArrayList<Double> doubles, ArrayList<Double> doubles2) throws Exception {
                DataOperation dataOPer=new DataOperation();
                return dataOPer.arrayPlu(doubles,doubles2);
            }
        }
        DataOperation dataOper=new DataOperation();
        ArrayList<Double> tmp=dataSet.map(new getpOWForAll()).reduce(new reduceArray());
        ArrayList<Double> var=dataOper.ArrayDivid(tmp,count);
        return var;
    }

    //获取整个数据集的最大值
    public static double getMax(JavaRDD<ElectricProfile> dataSet){
        class getMaxInDay implements Function<ElectricProfile,Double>{
            @Override
            public Double call(ElectricProfile electricProfile) throws Exception {
                double retValue=-1;
                for (int i = 0; i < electricProfile.getPoints().size(); i++) {
                    if (electricProfile.getPoints().get(i)>retValue){
                        retValue=electricProfile.getPoints().get(i);
                    }
                }
                return retValue;
            }
        }

        return  dataSet.map(new getMaxInDay()).top(1).get(0);
    }

    //获取整个数据集的最小值
    public static double getMin(JavaRDD<ElectricProfile> dataSet){
        class getMinInDay implements Function<ElectricProfile,Double>{
            @Override
            public Double call(ElectricProfile electricProfile) throws Exception {
                double retValue=Double.MAX_VALUE;
                for (int i = 0; i < electricProfile.getPoints().size(); i++) {
                    if (electricProfile.getPoints().get(i)<retValue){
                        retValue=electricProfile.getPoints().get(i);
                    }
                }
                return retValue;
            }
        }

        class sortKey implements Function<Double,Double>{
            @Override
            public Double call(Double aDouble) throws Exception {
                return aDouble;
            }
        }
        return  dataSet.map(new getMinInDay()).sortBy(new sortKey(),true,1).take(1).get(0);
    }

    public static List<Tuple2<Integer,ArrayList<Double>>> getMaxForCluster(JavaPairRDD<Integer,ElectricProfile> dataSet){

        class getMax implements Function2<ElectricProfile,ElectricProfile,ElectricProfile>{
            @Override
            public ElectricProfile call(ElectricProfile electricProfile, ElectricProfile electricProfile2) throws Exception {
                ArrayList<Double> tmpPoint=new ArrayList<Double>();
                for (int i = 0; i < electricProfile.getPoints().size(); i++) {
                    if (electricProfile.getPoints().get(i)>electricProfile2.getPoints().get(i)){
                        tmpPoint.add(electricProfile.getPoints().get(i));
                    }else {
                        tmpPoint.add(electricProfile2.getPoints().get(i));
                    }
                }
                ElectricProfile tmpEle=new ElectricProfile();
                tmpEle.setPoints(tmpPoint);
                return tmpEle;
            }
        }
        List<Tuple2<Integer,ArrayList<Double>>> retValue=new ArrayList<Tuple2<Integer, ArrayList<Double>>>();
        List<Tuple2<Integer,ElectricProfile>> maxDataSet=dataSet.reduceByKey(new getMax()).collect();
        for (int i = 0; i < maxDataSet.size(); i++) {
            retValue.add(new Tuple2<Integer, ArrayList<Double>>(maxDataSet.get(i)._1,maxDataSet.get(i)._2.getPoints()));
        }
        return retValue;
    }

    public static List<Tuple2<Integer,ArrayList<Double>>> getMinForCluster(JavaPairRDD<Integer,ElectricProfile> dataSet){
        class getMin implements Function2<ElectricProfile,ElectricProfile,ElectricProfile>{
            @Override
            public ElectricProfile call(ElectricProfile electricProfile, ElectricProfile electricProfile2) throws Exception {
                ArrayList<Double> tmpPoint=new ArrayList<Double>();
                for (int i = 0; i < electricProfile.getPoints().size(); i++) {
                    if (electricProfile.getPoints().get(i)<electricProfile2.getPoints().get(i)){
                        tmpPoint.add(electricProfile.getPoints().get(i));
                    }else {
                        tmpPoint.add(electricProfile2.getPoints().get(i));
                    }
                }
                ElectricProfile tmpEle=new ElectricProfile();
                tmpEle.setPoints(tmpPoint);
                return tmpEle;
            }
        }
        List<Tuple2<Integer,ArrayList<Double>>> retValue=new ArrayList<Tuple2<Integer, ArrayList<Double>>>();
        List<Tuple2<Integer,ElectricProfile>> minDataSet=dataSet.reduceByKey(new getMin()).collect();
        for (int i = 0; i < minDataSet.size(); i++) {
            retValue.add(new Tuple2<Integer, ArrayList<Double>>(minDataSet.get(i)._1,minDataSet.get(i)._2.getPoints()));
        }
        return retValue;

    }

    public static List<Tuple2<Integer,Tuple3<Double,Double,Double>>> getSumMeansMaxMinForCluster(JavaPairRDD<Integer,ElectricProfile> dataSet){

        class tranForCla implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<Tuple3<Double,Double,Double>,Long>>{
            @Override
            public Tuple2<Integer, Tuple2<Tuple3<Double, Double, Double>, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                double conSum=0;
                for (int i = 0; i < integerElectricProfileTuple2._2.getPoints().size(); i++) {
                    conSum+=integerElectricProfileTuple2._2.getPoints().get(i);
                }
                return new Tuple2<Integer, Tuple2<Tuple3<Double,Double,Double>, Long>>(
                        integerElectricProfileTuple2._1,
                        new Tuple2<Tuple3<Double, Double, Double>, Long>(
                                new Tuple3<Double, Double, Double>(
                                        conSum,conSum,conSum
                                ),1L
                        )
                );
            }
        }

        class plu implements Function2<Tuple2<Tuple3<Double,Double,Double>,Long>,Tuple2<Tuple3<Double,Double,Double>,Long>,Tuple2<Tuple3<Double,Double,Double>,Long>>{
            @Override
            public Tuple2<Tuple3<Double, Double, Double>, Long> call(Tuple2<Tuple3<Double, Double, Double>, Long> tuple3LongTuple2, Tuple2<Tuple3<Double, Double, Double>, Long> tuple3LongTuple22) throws Exception {

                double maxValue=tuple3LongTuple2._1._2();
                double minValue=tuple3LongTuple2._1._3();

                if (tuple3LongTuple2._1._2()<tuple3LongTuple22._1._2()){
                    maxValue=tuple3LongTuple22._1._2();
                }

                if (tuple3LongTuple2._1._3()>tuple3LongTuple22._1._3()){
                    minValue=tuple3LongTuple22._1._3();
                }

                return new Tuple2<Tuple3<Double, Double, Double>, Long>(new Tuple3<Double, Double, Double>(
                        tuple3LongTuple2._1._1()+tuple3LongTuple22._1._1(),maxValue,minValue
                ),tuple3LongTuple2._2+tuple3LongTuple22._2);
            }
        }

        class getMeans implements PairFunction<Tuple2<Integer,Tuple2<Tuple3<Double, Double, Double>, Long>>,Integer,Tuple3<Double,Double,Double>>{
            @Override
            public Tuple2<Integer, Tuple3<Double, Double, Double>> call(Tuple2<Integer, Tuple2<Tuple3<Double, Double, Double>, Long>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer, Tuple3<Double, Double, Double>>(integerTuple2Tuple2._1,
                        new Tuple3<Double, Double, Double>(
                                integerTuple2Tuple2._2._1._1()/integerTuple2Tuple2._2._2,
                                integerTuple2Tuple2._2._1._2(),
                                integerTuple2Tuple2._2._1._3()
                        ));
            }
        }

        return dataSet.mapToPair(new tranForCla()).reduceByKey(new plu()).mapToPair(new getMeans()).collect();
    }

    public static Map<Integer,Double> getSumVar(JavaPairRDD<Integer,ElectricProfile> dataSet,List<Tuple2<Integer,Tuple3<Double,Double,Double>>> means){
        Map<Integer,Double> meansSum=new HashMap<Integer, Double>();
        for (int i = 0; i < means.size(); i++) {
            meansSum.put(means.get(i)._1,means.get(i)._2._1());
        }

        final  Map<Integer,Double> meansSumCom=meansSum;
        class getSumCom implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Double>{
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                double conSum=0;
                for (int i = 0; i < integerElectricProfileTuple2._2.getPoints().size(); i++) {
                    conSum+=integerElectricProfileTuple2._2.getPoints().get(i);
                }
                return new Tuple2<Integer, Double>(integerElectricProfileTuple2._1,conSum);
            }
        }

        class getVar implements PairFunction<Tuple2<Integer,Double>,Integer,Tuple2<Double,Long>>{
            @Override
            public Tuple2<Integer, Tuple2<Double,Long>> call(Tuple2<Integer, Double> integerDoubleTuple2) throws Exception {
                return new Tuple2<Integer, Tuple2<Double,Long>>(integerDoubleTuple2._1,
                        new Tuple2<Double, Long>(Math.pow(integerDoubleTuple2._2-meansSumCom.get(integerDoubleTuple2._1),2),1L));
            }
        }

        class reduceForVar implements Function2<Tuple2<Double,Long>,Tuple2<Double,Long>,Tuple2<Double,Long>>{
            @Override
            public Tuple2<Double, Long> call(Tuple2<Double, Long> doubleLongTuple2, Tuple2<Double, Long> doubleLongTuple22) throws Exception {
                double value=doubleLongTuple2._1+doubleLongTuple22._1;
                long num=doubleLongTuple2._2+doubleLongTuple22._2;
                return new Tuple2<Double, Long>(value,num);
            }
        }

        class calculateVar implements PairFunction<Tuple2<Integer,Tuple2<Double,Long>>,Integer,Double>{
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Long>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer, Double>(integerTuple2Tuple2._1,integerTuple2Tuple2._2._1/integerTuple2Tuple2._2._2);
            }
        }

        return dataSet.mapToPair(new getSumCom()).mapToPair(new getVar()).reduceByKey(new reduceForVar()).mapToPair(new calculateVar()).collectAsMap();
    }


    //计算点的标准差
    public static Map<Integer,Double> getTheVarForPoint(HashMap<Integer,Double> means, JavaPairRDD<Integer,ElectricProfile> dataSet){
        final HashMap<Integer,Double> meansMap=means;
        class cla implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<Double,Long>>{
            @Override
            public Tuple2<Integer, Tuple2<Double, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                double _means=meansMap.get(integerElectricProfileTuple2._1);
                ArrayList<Double> data=integerElectricProfileTuple2._2().getPoints();
                double var=0;
                for (int i = 0; i <data.size(); i++) {
                    var+=Math.pow(data.get(i)-_means,2);
                }
                return new Tuple2<Integer, Tuple2<Double, Long>>(integerElectricProfileTuple2._1,new Tuple2<Double, Long>(var,1l));
            }
        }

        class reduceForVar implements Function2<Tuple2<Double, Long>,Tuple2<Double, Long>,Tuple2<Double, Long>>{
            @Override
            public Tuple2<Double, Long> call(Tuple2<Double, Long> doubleLongTuple2, Tuple2<Double, Long> doubleLongTuple22) throws Exception {
                return new Tuple2<Double, Long>(
                        doubleLongTuple2._1+doubleLongTuple22._1,
                        doubleLongTuple2._2+doubleLongTuple22._2
                );
            }
        }

        class getVar implements PairFunction<Tuple2<Integer,Tuple2<Double,Long>>,Integer,Double>{
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Long>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer,Double>(integerTuple2Tuple2._1,integerTuple2Tuple2._2._1/integerTuple2Tuple2._2._2);
            }
        }
        return dataSet.mapToPair(new cla()).reduceByKey(new reduceForVar()).mapToPair(new getVar()).collectAsMap();
    }
    //计算总用电量的平均标准差相对于平均总用电量的百分比
    public static void getuserinternalStd(JavaRDD<ElectricProfile> dataSet){
        class sumDay implements PairFunction<ElectricProfile,Long,Double>{
            @Override
            public Tuple2<Long, Double> call(ElectricProfile electricProfile) throws Exception {
                double tmpSum=0;
                for (int i = 0; i < electricProfile.getPoints().size(); i++) {
                    tmpSum+=electricProfile.getPoints().get(i);
                }
                return new Tuple2<Long, Double>(electricProfile.getID(),tmpSum);
            }
        }
        class calStd implements PairFunction<Tuple2<Long,Iterable<Double>>,Long,Double>{
            @Override
            public Tuple2<Long, Double> call(Tuple2<Long, Iterable<Double>> longIteratorTuple2) throws Exception {
                long count=0;
                double tmpSum=0;
                for (Double data:longIteratorTuple2._2
                     ) {
                    count++;
                    tmpSum+=data;
                }
                double tmpMeans=tmpSum/count;
                tmpSum=0;
                for (Double data:longIteratorTuple2._2
                     ) {
                    tmpSum+=(Math.pow(data-tmpMeans,2));
                }

                return new Tuple2<Long, Double>(longIteratorTuple2._1,Math.sqrt(tmpSum/count)/tmpMeans);
            }
        }

        JavaPairRDD<Long,Double> rateData=dataSet.mapToPair(new sumDay()).groupByKey().mapToPair(new calStd());
        rateData.cache();

        long userCount=rateData.count();
        class getData implements Function<Tuple2<Long,Double>,Double>{
            @Override
            public Double call(Tuple2<Long, Double> longDoubleTuple2) throws Exception {
                return longDoubleTuple2._2;
            }
        }

        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("sumStd "+rateData.map(new getData()).reduce(new ReduceDouble())/userCount);
        System.out.println("****************************************************");
        System.out.println("****************************************************");
        System.out.println("****************************************************");


    }

    public static Map<Integer,ArrayList<Double>> getMaxForClusterVerMap(JavaPairRDD<Integer,ElectricProfile> dataSet){

        class getMax implements Function2<ElectricProfile,ElectricProfile,ElectricProfile>{
            @Override
            public ElectricProfile call(ElectricProfile electricProfile, ElectricProfile electricProfile2) throws Exception {
                ArrayList<Double> tmpPoint=new ArrayList<Double>();
                for (int i = 0; i < electricProfile.getPoints().size(); i++) {
                    if (electricProfile.getPoints().get(i)>electricProfile2.getPoints().get(i)){
                        tmpPoint.add(electricProfile.getPoints().get(i));
                    }else {
                        tmpPoint.add(electricProfile2.getPoints().get(i));
                    }
                }
                ElectricProfile tmpEle=new ElectricProfile();
                tmpEle.setPoints(tmpPoint);
                return tmpEle;
            }
        }
        Map<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        List<Tuple2<Integer,ElectricProfile>> maxDataSet=dataSet.reduceByKey(new getMax()).collect();
        for (int i = 0; i < maxDataSet.size(); i++) {
            retValue.put(maxDataSet.get(i)._1,maxDataSet.get(i)._2.getPoints());
        }
        return retValue;
    }

    public static Map<Integer,ArrayList<Double>> getMinForClusterVerMap(JavaPairRDD<Integer,ElectricProfile> dataSet){
        class getMin implements Function2<ElectricProfile,ElectricProfile,ElectricProfile>{
            @Override
            public ElectricProfile call(ElectricProfile electricProfile, ElectricProfile electricProfile2) throws Exception {
                ArrayList<Double> tmpPoint=new ArrayList<Double>();
                for (int i = 0; i < electricProfile.getPoints().size(); i++) {
                    if (electricProfile.getPoints().get(i)<electricProfile2.getPoints().get(i)){
                        tmpPoint.add(electricProfile.getPoints().get(i));
                    }else {
                        tmpPoint.add(electricProfile2.getPoints().get(i));
                    }
                }
                ElectricProfile tmpEle=new ElectricProfile();
                tmpEle.setPoints(tmpPoint);
                return tmpEle;
            }
        }
        Map<Integer,ArrayList<Double>> retValue=new HashMap<Integer, ArrayList<Double>>();
        List<Tuple2<Integer,ElectricProfile>> minDataSet=dataSet.reduceByKey(new getMin()).collect();
        for (int i = 0; i < minDataSet.size(); i++) {
            retValue.put(minDataSet.get(i)._1,minDataSet.get(i)._2.getPoints());
        }
        return retValue;
    }

    public static Map<Integer,Tuple3<Double,Double,Double>> getSumMeansMaxMinForClusterVerMap(JavaPairRDD<Integer,ElectricProfile> dataSet){

        class tranForCla implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<Tuple3<Double,Double,Double>,Long>>{
            @Override
            public Tuple2<Integer, Tuple2<Tuple3<Double, Double, Double>, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                double conSum=0;
                for (int i = 0; i < integerElectricProfileTuple2._2.getPoints().size(); i++) {
                    conSum+=integerElectricProfileTuple2._2.getPoints().get(i);
                }
                return new Tuple2<Integer, Tuple2<Tuple3<Double,Double,Double>, Long>>(
                        integerElectricProfileTuple2._1,
                        new Tuple2<Tuple3<Double, Double, Double>, Long>(
                                new Tuple3<Double, Double, Double>(
                                        conSum,conSum,conSum
                                ),1L
                        )
                );
            }
        }

        class plu implements Function2<Tuple2<Tuple3<Double,Double,Double>,Long>,Tuple2<Tuple3<Double,Double,Double>,Long>,Tuple2<Tuple3<Double,Double,Double>,Long>>{
            @Override
            public Tuple2<Tuple3<Double, Double, Double>, Long> call(Tuple2<Tuple3<Double, Double, Double>, Long> tuple3LongTuple2, Tuple2<Tuple3<Double, Double, Double>, Long> tuple3LongTuple22) throws Exception {

                double maxValue=tuple3LongTuple2._1._2();
                double minValue=tuple3LongTuple2._1._3();

                if (tuple3LongTuple2._1._2()<tuple3LongTuple22._1._2()){
                    maxValue=tuple3LongTuple22._1._2();
                }

                if (tuple3LongTuple2._1._3()>tuple3LongTuple22._1._3()){
                    minValue=tuple3LongTuple22._1._3();
                }

                return new Tuple2<Tuple3<Double, Double, Double>, Long>(new Tuple3<Double, Double, Double>(
                        tuple3LongTuple2._1._1()+tuple3LongTuple22._1._1(),maxValue,minValue
                ),tuple3LongTuple2._2+tuple3LongTuple22._2);
            }
        }

        class getMeans implements PairFunction<Tuple2<Integer,Tuple2<Tuple3<Double, Double, Double>, Long>>,Integer,Tuple3<Double,Double,Double>>{
            @Override
            public Tuple2<Integer, Tuple3<Double, Double, Double>> call(Tuple2<Integer, Tuple2<Tuple3<Double, Double, Double>, Long>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer, Tuple3<Double, Double, Double>>(integerTuple2Tuple2._1,
                        new Tuple3<Double, Double, Double>(
                                integerTuple2Tuple2._2._1._1()/integerTuple2Tuple2._2._2,
                                integerTuple2Tuple2._2._1._2(),
                                integerTuple2Tuple2._2._1._3()
                        ));
            }
        }

        return dataSet.mapToPair(new tranForCla()).reduceByKey(new plu()).mapToPair(new getMeans()).collectAsMap();
    }

    public static Map<Integer,Integer> getClusterNum(JavaPairRDD<Integer,ElectricProfile> dataSet){

        class transToCount implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Integer>{
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                return new Tuple2<Integer, Integer>(integerElectricProfileTuple2._1,1);
            }
        }

        return dataSet.mapToPair(new transToCount()).reduceByKey(new ReduceInteger()).collectAsMap();
    }


    //获取每个用户的总用电量
    public static JavaPairRDD<Long, Double> getSum(JavaRDD<ElectricProfile> dataSet){

        class getEleID implements PairFunction<ElectricProfile,Long,ElectricProfile>{
            @Override
            public Tuple2<Long, ElectricProfile> call(ElectricProfile electricProfile) throws Exception {
                return new Tuple2<Long, ElectricProfile>(electricProfile.getID(),electricProfile);
            }
        }

        class getSum implements Function2<ElectricProfile,ElectricProfile,ElectricProfile>{
            @Override
            public ElectricProfile call(ElectricProfile electricProfile, ElectricProfile electricProfile2) throws Exception {
                ElectricProfile ele=new ElectricProfile();
                DataOperation doper=new DataOperation();
                ele.setPoints(doper.arrayPlu(electricProfile.getPoints(),electricProfile2.getPoints()));
                return ele;
            }
        }

        class getTotal implements PairFunction<Tuple2<Long,ElectricProfile>,Long,Double>{
            @Override
            public Tuple2<Long, Double> call(Tuple2<Long, ElectricProfile> longElectricProfileTuple2) throws Exception {
                ArrayList<Double> tmpValue=longElectricProfileTuple2._2.getPoints();
                double tmp=0;
                for (int i = 0; i < tmpValue.size(); i++) {
                    tmp+=tmpValue.get(i);
                }
                return new Tuple2<Long, Double>(longElectricProfileTuple2._1,tmp);
            }
        }

        return dataSet.mapToPair(new getEleID()).reduceByKey(new getSum()).mapToPair(new getTotal());
    }

    public ArrayList<Double> getKernelWidth(JavaRDD<ElectricProfile> dataSet){
        ArrayList<Double> var=variance(dataSet);
        final Long count=dataSet.count();
        int dimen=var.size();
        ArrayList<Double> widths=new ArrayList<Double>();
        for (int i = 0; i < dimen; i++) {
            double ithWidth=Math.pow(4/(2*dimen+1),1/(dimen+1))*var.get(i)*Math.pow(count,-1/(dimen+1));
            widths.add(ithWidth);
        }
        return  widths;
    }

    public void getEachCluster(JavaPairRDD<Integer,ElectricProfile> dataSet,int clusterNumb,String savePath){
        for (int i = 0; i < clusterNumb; i++) {
            final int filterFlag=i;
            class filiter implements Function<Tuple2<Integer,ElectricProfile>,Boolean>{
                @Override
                public Boolean call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                    return integerElectricProfileTuple2._1.equals(filterFlag);
                }
            }
            class getEle implements Function<Tuple2<Integer,ElectricProfile>,ElectricProfile>{
                @Override
                public ElectricProfile call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                    return integerElectricProfileTuple2._2;
                }
            }
            dataSet.filter(new filiter()).map(new getEle()).saveAsTextFile(savePath+"-"+i+"/");
        }
    }

    //获取DBI指标，聚类效果越好指标值越小
    public static double getDBI(JavaPairRDD<Integer,ElectricProfile> dataSet,ArrayList<ElectricProfile> centers ){

       final ArrayList<ElectricProfile> center=centers;

        class claculate implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<Double,Long>>{
            @Override
            public Tuple2<Integer, Tuple2<Double, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                Distance dist=new Distance();
                return new Tuple2<Integer, Tuple2<Double, Long>>(
                        integerElectricProfileTuple2._1,
                        new Tuple2<Double, Long>(
                               dist.getPowDistance(integerElectricProfileTuple2._2.getPoints(),center.get(integerElectricProfileTuple2._1).getPoints()),1L
                        )
                );
            }
        }

        class pluEle implements Function2<Tuple2<Double,Long>,Tuple2<Double,Long>,Tuple2<Double,Long>>{
            @Override
            public Tuple2<Double, Long> call(Tuple2<Double, Long> doubleLongTuple2, Tuple2<Double, Long> doubleLongTuple22) throws Exception {
                return new Tuple2<Double, Long>(
                        doubleLongTuple2._1+doubleLongTuple22._1,doubleLongTuple2._2+doubleLongTuple22._2
                );
            }
        }

        class calSI implements PairFunction<Tuple2<Integer,Tuple2<Double,Long>>,Integer,Double>{
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Tuple2<Double, Long>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer,Double>(
                        integerTuple2Tuple2._1,
                        Math.sqrt(integerTuple2Tuple2._2._1/integerTuple2Tuple2._2._2)
                );
            }
        }

        List<Tuple2<Integer,Double>> SI=dataSet.mapToPair(new claculate()).reduceByKey(new pluEle()).mapToPair(new calSI()).collect();
        double BDI=0;
        for (int i = 0; i < SI.size(); i++) {
            Tuple2<Integer,Double> tmpSI=SI.get(i);
            double maxR=0;
            for (int j = 0; j < SI.size(); j++) {
                if (i!=j){
                    Tuple2<Integer,Double> tmpSI2=SI.get(j);
                    Distance dist=new Distance();
                    double tmpDistCenters=dist.getEuclideanDistance(center.get(tmpSI._1).getPoints(),center.get(tmpSI2._1).getPoints());
                    //System.out.print(tmpSI._1+"-"+tmpSI2._1+":    "+tmpDistCenters+"\n");
                    double tmpR=(tmpSI._2+tmpSI2._2)/tmpDistCenters;
                    System.out.println();
                    if (tmpR>maxR){
                        maxR=tmpR;
                    }
                }
            }
            //double tmpKK=tmpSI._2;
            //System.out.print("index-"+i+":  "+tmpKK+"\n");
            BDI+=maxR;
        }
       return BDI/center.size();
    }

    //获取聚类的平均方差
    public static double getMeanVar(JavaPairRDD<Integer,ElectricProfile> dataSet,ArrayList<ElectricProfile> centers){
        final ArrayList<ElectricProfile> center=centers;
        class getEachVar implements Function<Tuple2<Integer,ElectricProfile>,Double>{
            @Override
            public Double call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                Distance dist=new Distance();
                double tmp=dist.getPowDistance(integerElectricProfileTuple2._2.getPoints(),center.get(integerElectricProfileTuple2._1).getPoints());
                return tmp;
            }
        }

        long dataSetCount=dataSet.count();
        return dataSet.map(new getEachVar()).reduce(new ReduceDouble())/dataSetCount;
    }

    //获取每一个类的每一个时间点上的平均值
    public static Map<Integer,ArrayList<Double>> getMeansForCluster(JavaPairRDD<Integer,ElectricProfile> dataSet){
        class transTopair implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Tuple2<ArrayList<Double>,Long>>{
            @Override
            public Tuple2<Integer, Tuple2<ArrayList<Double>, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                return new Tuple2<Integer, Tuple2<ArrayList<Double>, Long>>(
                        integerElectricProfileTuple2._1,
                        new Tuple2<ArrayList<Double>, Long>(
                                integerElectricProfileTuple2._2.getPoints(),1L
                        )
                );
            }
        }
        class pul implements Function2<Tuple2<ArrayList<Double>,Long>,Tuple2<ArrayList<Double>,Long>,Tuple2<ArrayList<Double>,Long>>{
            @Override
            public Tuple2<ArrayList<Double>, Long> call(Tuple2<ArrayList<Double>, Long> arrayListLongTuple2, Tuple2<ArrayList<Double>, Long> arrayListLongTuple22) throws Exception {
                DataOperation dop=new DataOperation();
                return new Tuple2<ArrayList<Double>, Long>(
                        dop.arrayPlu(arrayListLongTuple2._1,arrayListLongTuple22._1),arrayListLongTuple2._2+arrayListLongTuple22._2
                );
            }
        }
        class getMeans implements PairFunction<Tuple2<Integer,Tuple2<ArrayList<Double>,Long>>,Integer,ArrayList<Double>>{
            @Override
            public Tuple2<Integer, ArrayList<Double>> call(Tuple2<Integer, Tuple2<ArrayList<Double>, Long>> integerTuple2Tuple2) throws Exception {
                DataOperation dop=new DataOperation();
                return new Tuple2<Integer, ArrayList<Double>>(
                        integerTuple2Tuple2._1,
                        dop.ArrayDivid(integerTuple2Tuple2._2._1,Double.parseDouble(String.valueOf(integerTuple2Tuple2._2._2)))
                );
            }
        }
        JavaPairRDD<Integer,Tuple2<ArrayList<Double>,Long>> tDataSet=dataSet.mapToPair(new transTopair());
        return tDataSet.reduceByKey(new pul()).mapToPair(new getMeans()).collectAsMap();
    }

    //获取每一个类每一个时间点上的方差
    public static List<Tuple2<Integer,ArrayList<Double>>> getVarianceForCluster(JavaPairRDD<Integer,ElectricProfile> dataSet,Map<Integer,ArrayList<Double>> meansArray){
        final Map<Integer,ArrayList<Double>> finalMeans=meansArray;

        class getPowVC implements PairFunction<Tuple2<Integer,ElectricProfile> ,Integer,ArrayList<Double>>{
            @Override
            public Tuple2<Integer, ArrayList<Double>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                DataOperation doper=new DataOperation();
                return new Tuple2<Integer, ArrayList<Double>>(
                        integerElectricProfileTuple2._1,
                        doper.arrayPow(doper.arrayMinus(integerElectricProfileTuple2._2.getPoints(),finalMeans.get(integerElectricProfileTuple2._1)))
                );
            }
        }

        class trantoCount implements PairFunction<Tuple2<Integer,ArrayList<Double>>,Integer,Tuple2<ArrayList<Double>,Long>>{
            @Override
            public Tuple2<Integer, Tuple2<ArrayList<Double>, Long>> call(Tuple2<Integer, ArrayList<Double>> integerArrayListTuple2) throws Exception {
                return new Tuple2<Integer, Tuple2<ArrayList<Double>, Long>>(
                        integerArrayListTuple2._1,
                        new Tuple2<ArrayList<Double>, Long>(integerArrayListTuple2._2,1L)
                );
            }
        }

        class pluByKey implements Function2<Tuple2<ArrayList<Double>,Long>,Tuple2<ArrayList<Double>,Long>,Tuple2<ArrayList<Double>,Long>>{
            @Override
            public Tuple2<ArrayList<Double>, Long> call(Tuple2<ArrayList<Double>, Long> arrayListLongTuple2, Tuple2<ArrayList<Double>, Long> arrayListLongTuple22) throws Exception {
                DataOperation doper=new DataOperation();
                return new Tuple2<ArrayList<Double>, Long>(
                        doper.arrayPlu(arrayListLongTuple2._1,arrayListLongTuple22._1),arrayListLongTuple2._2+arrayListLongTuple22._2
                );
            }
        }

        class getVar implements PairFunction<Tuple2<Integer,Tuple2<ArrayList<Double>,Long>>,Integer,ArrayList<Double>>{
            @Override
            public Tuple2<Integer, ArrayList<Double>> call(Tuple2<Integer, Tuple2<ArrayList<Double>, Long>> integerTuple2Tuple2) throws Exception {
                DataOperation doper=new DataOperation();
                return new Tuple2<Integer, ArrayList<Double>>(
                        integerTuple2Tuple2._1,
                        doper.ArrayDivid(integerTuple2Tuple2._2._1,integerTuple2Tuple2._2._2)
                );
            }
        }
        return dataSet.mapToPair(new getPowVC()).mapToPair(new trantoCount()).reduceByKey(new pluByKey()).mapToPair(new getVar()).collect();
    }

    //获取所有类的平均方差
    public static double getMeansVar(List<Tuple2<Integer,ArrayList<Double>>> var){
        double retValue=0;
        DataOperation doper=new DataOperation();
        for (int i = 0; i < var.size(); i++) {
            retValue+=doper.getArrayModel(var.get(i)._2);
        }
        retValue=Math.sqrt(retValue)/var.size();
        return retValue;
    }

    //获取S_Dbw指标，聚类效果越好指标值越小
    public static double getS_Dbw(JavaPairRDD<Integer,ElectricProfile> dataSet, ArrayList<ElectricProfile> centers, JavaSparkContext sc ){
        dataSet.cache();
        DataOperation doper=new DataOperation();
        final ArrayList<ElectricProfile> fineCenters=centers;
        class trans implements Function<Tuple2<Integer,ElectricProfile>,ElectricProfile>{
            @Override
            public ElectricProfile call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                return integerElectricProfileTuple2._2;
            }
        }

        Map<Integer,ArrayList<Double>> clusterMeans=getMeansForCluster(dataSet);
        List<Tuple2<Integer,ArrayList<Double>>> clusterVar=getVarianceForCluster(dataSet,clusterMeans);
        final double stdev=getMeansVar(clusterVar);

        //region    dens_bw
        class isNer implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,Long>{
            @Override
            public Tuple2<Integer, Long> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                Distance dist=new Distance();
                double tmpDist=dist.getEuclideanDistance(integerElectricProfileTuple2._2.getPoints(),fineCenters.get(integerElectricProfileTuple2._1).getPoints());
                long ner=0;
                if (tmpDist<=stdev){
                    ner=1;
                }
                return new Tuple2<Integer,Long>(integerElectricProfileTuple2._1,ner);
            }
        }

        class pulDensity implements Function2<Long,Long,Long>{
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        }


        double dens_bw=0;
        HashMap<Integer,Long> centerDensity=new HashMap<Integer, Long>();
        Map<Integer,Long> tmpCenterDensity=dataSet.mapToPair(new isNer()).reduceByKey(new pulDensity()).collectAsMap();
        Iterator iter = tmpCenterDensity.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer,Long> entry = (Map.Entry<Integer,Long>) iter.next();
            Integer key=entry.getKey();
            Long value=entry.getValue();
            centerDensity.put(key,value);
        }
        for (int i = 0; i < centers.size(); i++) {
            if (!centerDensity.containsKey(i)){
                centerDensity.put(i,0L);
            }
        }
        List<Tuple2<String,ArrayList<Double>>> partMid=new ArrayList<Tuple2<String, ArrayList<Double>>>();
        for (int i = 0; i < centers.size(); i++) {
            for (int j = i+1; j <centers.size() ; j++) {
                partMid.add(new Tuple2<String, ArrayList<Double>>(
                        i+"-"+j,
                        doper.ArrayDivid(doper.arrayPlu(centers.get(i).getPoints(),centers.get(j).getPoints()),2)
                ));
                if (partMid.size()>=10000){
                    List<Tuple2<String,Long>> midDensity=getDensityForMid(dataSet,partMid,stdev,sc);
                    partMid.clear();
                    for (int k = 0; k < midDensity.size(); k++) {
                        long midDen=midDensity.get(k)._2;
                        int index1=Integer.parseInt(midDensity.get(k)._1.split("-")[0]);
                        int index2=Integer.parseInt(midDensity.get(k)._1.split("-")[1]);

                        long fDen=centerDensity.get(index1);
                        long sDen=centerDensity.get(index2);

                        long maxDen=1;
                        if (fDen>sDen){
                            maxDen=fDen;
                        }else {
                            maxDen=sDen;
                        }

                        if (maxDen<=0){
                            maxDen=1;
                        }
                        dens_bw+=midDen/maxDen;
                    }
                }
            }
        }

        if (partMid.size()>0){
            List<Tuple2<String,Long>> midDensity=getDensityForMid(dataSet,partMid,stdev,sc);
            partMid.clear();
            for (int k = 0; k < midDensity.size(); k++) {
                long midDen=midDensity.get(k)._2;
                int index1=Integer.parseInt(midDensity.get(k)._1.split("-")[0]);
                int index2=Integer.parseInt(midDensity.get(k)._1.split("-")[1]);

                long fDen=centerDensity.get(index1);
                long sDen=centerDensity.get(index2);

                long maxDen=1;
                if (fDen>sDen){
                    maxDen=fDen;
                }else {
                    maxDen=sDen;
                }

                if (maxDen<=0){
                    maxDen=1;
                }
                dens_bw+=midDen/maxDen;
            }
        }
        dens_bw=2*dens_bw/(centers.size()*(centers.size()-1));
        //endregion

        double varCenters=0;
        for (int i = 0; i < clusterVar.size(); i++) {
            varCenters+=doper.getArrayModel(clusterVar.get(i)._2);
        }

        double dataSetVar=doper.getArrayModel(variance(dataSet.map(new trans())));

        double scat=varCenters/(centers.size()*dataSetVar);

        return dens_bw+scat;
    }

    //计算S_Dbw的中间点的密度
    public static List<Tuple2<String,Long>> getDensityForMid(JavaPairRDD<Integer,ElectricProfile> dataSet,List<Tuple2<String,ArrayList<Double>>> partMid,double nerThr,JavaSparkContext sc){
        final Broadcast<List<Tuple2<String,ArrayList<Double>>>> broPartDataSet=sc.broadcast(partMid);
        final double stdev=nerThr;
        class isNer implements PairFlatMapFunction<Tuple2<Integer,ElectricProfile>,String,Long>{
            @Override
            public Iterable<Tuple2<String, Long>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                List<Tuple2<String,ArrayList<Double>>> partDataTable=broPartDataSet.getValue();
                List<Tuple2<String,Long>> retValue=new ArrayList<Tuple2<String,Long>>();
                for (Tuple2<String,ArrayList<Double>> data:partDataTable
                     ) {
                    int index1=Integer.parseInt(data._1.split("-")[0]);
                    int index2=Integer.parseInt(data._1.split("-")[1]);
                    if ((integerElectricProfileTuple2._1.equals(index1))||(integerElectricProfileTuple2._1.equals(index2))){
                        Distance dist=new Distance();
                        double tmpDist=dist.getEuclideanDistance(integerElectricProfileTuple2._2.getPoints(),data._2);
                        if (tmpDist<=stdev){
                            retValue.add(new Tuple2<String, Long>(data._1,1L));
                        }
                        //else {
                            //retValue.add(new Tuple2<String, Long>(data._1,0L));
                        //}
                    }
                }
                return retValue;
            }
        }

        class pulDen implements Function2<Long,Long,Long>{
            @Override
            public Long call(Long aLong, Long aLong2) throws Exception {
                return aLong+aLong2;
            }
        }

        return dataSet.flatMapToPair(new isNer()).reduceByKey(new pulDen()).collect();
    }

    //获取聚类中心
    public static ArrayList<ArrayList<Double>> getCenters(String path) throws IOException {

        ArrayList<ArrayList<Double>> centers=new ArrayList<ArrayList<Double>>();
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
                    ArrayList<Double> tmpCents=new ArrayList<Double>();
                    String[] tmpdistArray = line.split(",");
                    for (int j = 1; j < tmpdistArray.length; j++) {
                        tmpCents.add(Double.parseDouble(tmpdistArray[j]));
                    }
                    centers.add(tmpCents);
                }
            }
        }
        if (in!=null){
            in.close();
        }
        if (fsi!=null){
            fsi.close();
        }

        return centers;
    }


    public static ArrayList<ElectricProfile> getCentersWithELE(String path) throws IOException {

        ArrayList<ElectricProfile> centers=new ArrayList<ElectricProfile>();
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
                    ArrayList<Double> tmpCents=new ArrayList<Double>();
                    String[] tmpdistArray = line.split(",");
                    for (int j = 1; j < tmpdistArray.length; j++) {
                        tmpCents.add(Double.parseDouble(tmpdistArray[j]));
                    }
                    ElectricProfile tmpELE=new ElectricProfile();
                    tmpELE.setPoints(tmpCents);
                    centers.add(tmpELE);
                }
            }
        }
        if (in!=null){
            in.close();
        }
        if (fsi!=null){
            fsi.close();
        }

        return centers;
    }


    //初始化距离矩阵等
    public void initial(String pcPath,String wcPath,int flag,int calculateFlag) throws IOException {
        ArrayList<ArrayList<Double>> pcDic=new ArrayList<ArrayList<Double>>();//日模式字典
        ArrayList<ArrayList<Integer>> wcDic=new ArrayList<ArrayList<Integer>>();//周模式字典

        FileSystem fs = FileSystem.get(URI.create(pcPath),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(pcPath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        for(int i = 0; i < fileList.length; i++) {
            if (!fileList[i].isDirectory()) {
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
                while ((line = in.readLine()) != null) {
                    String[] tmpdistArray = line.split(",");
                    ArrayList<Double> tmpPc=new ArrayList<Double>();
                    for (int j = 1; j < tmpdistArray.length; j++) {
                        tmpPc.add(Double.parseDouble(tmpdistArray[j]));
                    }
                    pcDic.add(tmpPc);
                }
            }
        }
        this.pcSize=pcDic.size();
        FloatMatrix pcMatrix=new FloatMatrix(pcDic.size());
        for (int i = 0; i < pcDic.size(); i++) {
            for (int j = 0; j < pcDic.size(); j++) {
                if (i<j){
                    double tmpDist=Double.MAX_VALUE;
                    if (calculateFlag==0){//calculateFlag表示距离的计算方式，0表示动态扭曲计算，1表示形状距离
                        Distance dist=new Distance();
                        tmpDist=dist.getDTWDistance(pcDic.get(i),pcDic.get(j));
                    }else if (calculateFlag==1){
                        Tuple2<Integer,Double> tmp= FftConv.getMaxNNCc(pcDic.get(i),pcDic.get(j));
                        tmpDist=1-tmp._2;
                    }else {
                        Distance dist=new Distance();
                        tmpDist= dist.getEuclideanDistance(pcDic.get(i),pcDic.get(j));
                    }
                    pcMatrix.setValue((float) tmpDist,i,j,pcDic.size());
                }
            }
        }
        this.pcDistMatrix=pcMatrix;

        if (in!=null){
            in.close();
        }
        if (fsi!=null){
            fsi.close();
        }

        if (flag==1){//flag=1表示用于年聚类,需要初始化周字典距离矩阵，flag=0表示用于周聚类
            FileSystem fsw = FileSystem.get(URI.create(wcPath),new Configuration());
            FileStatus[] fileListw = fsw.listStatus(new Path(wcPath));
            BufferedReader inw = null;
            FSDataInputStream fsiw = null;
            String linew = null;
            for(int i = 0; i < fileListw.length; i++) {
                if (!fileListw[i].isDirectory()) {
                    fsiw = fsw.open(fileListw[i].getPath());
                    inw = new BufferedReader(new InputStreamReader(fsiw, "UTF-8"));
                    while ((linew = inw.readLine()) != null) {
                        String[] tmpdistArray = linew.split(",");
                        ArrayList<Integer> tmpWc=new ArrayList<Integer>();
                        for (int j = 1; j < tmpdistArray.length; j++) {
                            tmpWc.add(Integer.parseInt(tmpdistArray[j]));
                        }
                        wcDic.add(tmpWc);
                    }
                }
            }

            FloatMatrix wcMatrix=new FloatMatrix(wcDic.size());
            this.wcSize=wcDic.size();
            for (int i = 0; i < wcDic.size(); i++) {
                for (int j = 0; j < wcDic.size(); j++) {
                    if (i<j){
                        ArrayList<Integer> tmpWc1=wcDic.get(i);
                        ArrayList<Integer> tmpWc2=wcDic.get(j);
                        double tmpDist=0;
                        for (int k = 0; k <tmpWc1.size() ; k++) {
                            tmpDist+=Math.pow(pcMatrix.getValue(tmpWc1.get(k),tmpWc2.get(k), pcDic.size()),2);
                        }
                        tmpDist=Math.sqrt(tmpDist);
                        wcMatrix.setValue((float) tmpDist,i,j,wcDic.size());
                    }
                }
            }
            this.wcDistMatrix=wcMatrix;


            if (inw!=null){
                inw.close();
            }
            if (fsiw!=null){
                fsiw.close();
            }
        }

    }


    public static double getMeansDiistan(String pcPath,String wcPath ,JavaRDD<WeekAndYearInstance> dataSet,int wyFlag,int calDistFlag,int yFlage,JavaSparkContext sc) throws IOException {

        DigitalFeature seg=new DigitalFeature();
        seg.initial(pcPath,wcPath,wyFlag,calDistFlag);
        //final FloatMatrix distPcMatrix=seg.pcDistMatrix;
        //final FloatMatrix distWcMatrix=seg.wcDistMatrix;
        final Broadcast<FloatMatrix> broDistPcMatrix=sc.broadcast(seg.pcDistMatrix);
        final Broadcast<FloatMatrix> broDistWcMatrix=sc.broadcast(seg.wcDistMatrix);

        final int _pcSize=seg.pcSize;
        final int _wcSize=seg.wcSize;

        long count=dataSet.count();
        List<WeekAndYearInstance> dataSample;
        if (count>500){
            dataSample=dataSet.takeSample(false,500,30);
        }else {
            dataSample=dataSet.collect();
        }

        final Broadcast<List<WeekAndYearInstance>> broPartDataSet=sc.broadcast(dataSample);
        final int wyFlags=wyFlag;
        final int yearCalFlag=yFlage;

        class claDistTan implements FlatMapFunction<WeekAndYearInstance,Double>{
            @Override
            public Iterable<Double> call(WeekAndYearInstance weekAndYearInstance) throws Exception {
                List<WeekAndYearInstance> partDataTable=broPartDataSet.getValue();
                FloatMatrix distPcMatrix=broDistPcMatrix.getValue();
                FloatMatrix distWcMatrix=broDistWcMatrix.getValue();
                ArrayList<Double> dist=new ArrayList<Double>();
                for (WeekAndYearInstance data:partDataTable
                     ) {
                    ArrayList<Integer> points=data.getData();
                    double tmpDist=0;
                    if (wyFlags==0){

                        for (int i = 0; i < points.size(); i++) {
                            tmpDist+=Math.pow(distPcMatrix.getValue(points.get(i),weekAndYearInstance.getData().get(i),_pcSize),2);
                        }
                        tmpDist=Math.sqrt(tmpDist);

                    }else {
                        if (yearCalFlag==0){
                            DTWDistanForScalar dtwDist=new DTWDistanForScalar();
                            //tmpDist= dtwDist.getDTWDistanceForScalar(points,weekAndYearInstance.getData(),distWcMatrix);
                            tmpDist= dtwDist.getDTWDistanceForScalar(points,weekAndYearInstance.getData(),distPcMatrix,_wcSize);
                        }else {
                            for (int i = 0; i < points.size(); i++) {
                                //tmpDist+=Math.pow(distWcMatrix.getAsDouble(points.get(i),weekAndYearInstance.getData().get(i)),2);
                                tmpDist+=Math.pow(distWcMatrix.getValue(points.get(i),weekAndYearInstance.getData().get(i),_wcSize),2);
                            }
                            tmpDist=Math.sqrt(tmpDist);
                        }
                    }
                    dist.add(tmpDist);
                }
                return dist;
            }
        }

        class pul implements Function2<Double,Double,Double>{
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return aDouble+aDouble2;
            }
        }
        JavaRDD<Double> distSet=dataSet.flatMap(new claDistTan());
        class trantoPair implements PairFunction<Double,Double,Integer>{
            @Override
            public Tuple2<Double, Integer> call(Double aDouble) throws Exception {
                return new Tuple2<Double, Integer>(aDouble,1);
            }
        }

        class filter implements Function<Tuple2<Double,Integer>,Boolean>{
            @Override
            public Boolean call(Tuple2<Double, Integer> doubleIntegerTuple2) throws Exception {
                return !doubleIntegerTuple2._1.equals(0);
            }
        }
        JavaPairRDD<Double,Integer> sortPairDataSet=distSet.mapToPair(new trantoPair()).filter(new filter());

        distSet.cache();
        double total=distSet.reduce(new pul());
        double max=distSet.top(2).get(1);
        double min=sortPairDataSet.sortByKey().take(5).get(4)._1;
        long sampleCount=dataSample.size();

        System.out.println("*******************************************");
        System.out.println("*******************************************");
        System.out.println("*******************************************");
        System.out.println("");
        System.out.println("Mean:  "+total/(sampleCount*count));
        System.out.println("Max:  "+max);
        System.out.println("Min:  "+min);
        System.out.println("");
        System.out.println("*******************************************");
        System.out.println("*******************************************");
        System.out.println("*******************************************");

        return total/(sampleCount*count);
    }

    public  void getStaDig(JavaPairRDD<Integer,ElectricProfile> dataSet,int pointNum,String Path,int flag) throws IOException, InterruptedException {
        String retValue="ClusterID,ClusterNum,Mean,MaxMean,MaxMeanIdex,MinMean,MinMeanIdex,Var,Std,MaxVar,MaxVarIndex,MinVar,MinVarIndex,Max,MaxIndex,Min,MinIndex" +
                ",SumMeans,SumVar,SumStd,SumStd2Mean,SumMax,SumMin,pointVar,pointStd";
        //region
        //String retValue="ClusterID,ClusterNum,Mean,Std,Max,MaxIndex,Min,MinIndex,SumMeans,SumStd,SumStd2Mean,SumMax,SumMin";
        /*
        if (flag==0){
            for (int i = 0; i < pointNum; i++) {
                retValue+=",Mean-"+i;
                retValue+=",Mean-"+i+"-index";
            }
            for (int i = 0; i < pointNum; i++) {
                retValue+=",Var-"+i;
                retValue+=",Var-"+i+"-index";
            }
            for (int i = 0; i < pointNum; i++) {
                retValue+=",Max-"+i;
                retValue+=",Max-"+i+"-index";
            }
            for (int i = 0; i < pointNum; i++) {
                retValue+=",Min-"+i;
                retValue+=",Min-"+i+"-index";
            }
        }

        for (int i = 0; i < pointNum; i++) {
            retValue+=",Mean-"+i;
        }
        for (int i = 0; i < pointNum; i++) {
            retValue+=",Var-"+i;
        }
        for (int i = 0; i < pointNum; i++) {
            retValue+=",Max-"+i;
        }
        for (int i = 0; i < pointNum; i++) {
            retValue+=",Min-"+i;
        }
        */
        //endregion
        retValue+="\n";
        HDFSOperate.writeToHdfs(Path,retValue);

        Map<Integer,ArrayList<Double>> means=getMeansForCluster(dataSet);
        List<Tuple2<Integer,ArrayList<Double>>> var=getVarianceForCluster(dataSet,means);
        List<Tuple2<Integer,ArrayList<Double>>> max=getMaxForCluster(dataSet);
        List<Tuple2<Integer,ArrayList<Double>>> min=getMinForCluster(dataSet);
        List<Tuple2<Integer,Tuple3<Double,Double,Double>>> sum=getSumMeansMaxMinForCluster(dataSet);
        Map<Integer,Double> sumVar=getSumVar(dataSet,sum);
        Map<Integer,Integer> clusterNumber=getClusterNum(dataSet);
        HashMap<Integer,ArrayList<Double>> varMap=new HashMap<Integer, ArrayList<Double>>();
        HashMap<Integer,ArrayList<Double>> maxMap=new HashMap<Integer, ArrayList<Double>>();
        HashMap<Integer,ArrayList<Double>> minMap=new HashMap<Integer, ArrayList<Double>>();
        HashMap<Integer,Double> meansMap=new HashMap<Integer, Double>();

        for (int i = 0; i < sum.size(); i++) {
            varMap.put(var.get(i)._1,var.get(i)._2);
            maxMap.put(max.get(i)._1,max.get(i)._2);
            minMap.put(min.get(i)._1,min.get(i)._2);

        }

        for (Tuple2<Integer,Tuple3<Double,Double,Double>> data:sum
             ) {
            meansMap.put(data._1,data._2()._1()/pointNum);
        }

        Map<Integer,Double> pointVar=getTheVarForPoint(meansMap,dataSet);
        DigitalFeature df=new DigitalFeature();
        WritText wt=new WritText();

        int thrCount=0;
        while (thrCount<sum.size()){
            ExecutorService exe = Executors.newFixedThreadPool(2005);
            for (int i = 0; i < 2000; i++) {
                if (thrCount<sum.size()){
                    writThread tmpThr=df.new writThread(String.valueOf(thrCount),thrCount,flag,Path,means,sum,sumVar,
                            clusterNumber,varMap,maxMap,minMap,wt,pointVar);
                    exe.execute(tmpThr);
                    thrCount++;
                }
            }
            exe.shutdown();
            while (true) {
                if (exe.isTerminated()) {
                    System.out.println(thrCount+"   结束了！");
                    break;
                }
                Thread.sleep(200);
            }
        }

        System.out.println("程序结束了！");

    }

    public class WritText {

        public void write(String retValueStr,String path) throws IOException {
            HDFSOperate.writeToHdfs(path,retValueStr);
        }
    }

    class writThread extends Thread{
        private Map<Integer,ArrayList<Double>> means;
        private List<Tuple2<Integer,Tuple3<Double,Double,Double>>> sum;
        private Map<Integer,Double> sumVar;
        private Map<Integer,Integer> clusterNumber;
        private HashMap<Integer,ArrayList<Double>> varMap;
        private HashMap<Integer,ArrayList<Double>> maxMap;
        private HashMap<Integer,ArrayList<Double>> minMap;
        private int index;
        private int flag;
        private String Path;
        private WritText wt;
        private Map<Integer,Double> pointVar;

        public writThread( String name,int index,int flag,String path,
                Map<Integer,ArrayList<Double>> means,
                List<Tuple2<Integer,Tuple3<Double,Double,Double>>> sum,
                Map<Integer,Double> sumVar,
                Map<Integer,Integer> clusterNumber,
                HashMap<Integer,ArrayList<Double>> varMap,
                HashMap<Integer,ArrayList<Double>> maxMap,
                HashMap<Integer,ArrayList<Double>> minMap, WritText wt,Map<Integer,Double> pointVar){
            super(name);
            this.means=means;
            this.sum=sum;
            this.sumVar=sumVar;
            this.clusterNumber=clusterNumber;
            this.varMap=varMap;
            this.maxMap=maxMap;
            this.minMap=minMap;
            this.index=index;
            this.flag=flag;
            this.Path=path;
            this.wt=wt;
            this.pointVar=pointVar;
        }

        public void run(){

            String retValue="";
            DataOperation doper=new DataOperation();
            int clusterNum=sum.get(index)._1;
            retValue+=clusterNum+",";
            retValue+=clusterNumber.get(clusterNum)+",";
            retValue+=doper.getArrayMean(means.get(clusterNum));
            retValue+=","+doper.getArrayMaxWithIndex(means.get(clusterNum));
            retValue+=","+doper.getArrayMinWithIndex(means.get(clusterNum));
            retValue+=","+doper.getArrayModel(varMap.get(clusterNum));
            retValue+=","+Math.sqrt(doper.getArrayModel(varMap.get(clusterNum)));
            retValue+=","+doper.getArrayMaxWithIndex(varMap.get(clusterNum));
            retValue+=","+doper.getArrayMinWithIndex(varMap.get(clusterNum));
            retValue+=","+doper.getArrayMaxWithIndex(maxMap.get(clusterNum));
            retValue+=","+doper.getArrayMinWithIndex(minMap.get(clusterNum));
            retValue+=","+sum.get(index)._2._1();
            retValue+=","+sumVar.get(clusterNum);
            retValue+=","+Math.sqrt(sumVar.get(clusterNum));
            retValue+=","+(Math.sqrt(sumVar.get(clusterNum))/sum.get(index)._2._1());
            retValue+=","+sum.get(index)._2._2();
            retValue+=","+sum.get(index)._2._3();
            retValue+=","+pointVar.get(clusterNum);
            retValue+=","+Math.sqrt(pointVar.get(clusterNum));
            /*
            if (flag==0){
                retValue+=","+doper.listTupleidToStr(doper.sortArray(means.get(clusterNum)));
                retValue+=","+doper.listTupleidToStr(doper.sortArray(varMap.get(clusterNum)));
                retValue+=","+doper.listTupleidToStr(doper.sortArray(maxMap.get(clusterNum)));
                retValue+=","+doper.listTupleidToStr(doper.sortArray(minMap.get(clusterNum)));
            }
            retValue+=","+doper.ArrayToStr(means.get(clusterNum));
            retValue+=","+doper.ArrayToStr(varMap.get(clusterNum));
            retValue+=","+doper.ArrayToStr(maxMap.get(clusterNum));
            retValue+=","+doper.ArrayToStr(minMap.get(clusterNum));
            */
            retValue+="\n";
            synchronized (wt){
                try {
                    wt.write(retValue,Path);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {


        String inputPath=args[0];
        String pcPath=args[1];
        String wcPath=args[2];
        int wyFlag=Integer.parseInt(args[3]);
        int calDistFlag=Integer.parseInt(args[4]);
        int yearCalDist=Integer.parseInt(args[5]);

        SparkConf conf=new SparkConf().setAppName("DIG");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(inputPath);
        class split implements Function<String,WeekAndYearInstance>{
            @Override
            public WeekAndYearInstance call(String s) throws Exception {
                return new WeekAndYearInstance(s);
            }
        }
        getMeansDiistan(pcPath,wcPath,lines.map(new split()),wyFlag,calDistFlag,yearCalDist,sc);

    }




}
