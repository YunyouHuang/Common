package com.Data;

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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hdp on 16-11-6.
 */
public class CreateGraph3V {



    public static void getEdgeSet(JavaPairRDD<Long,ArrayList<Double>> dataSet, String inputPath, String HDFSOutputPath, int distCalculateFlag, int calculateBlockSize, JavaSparkContext sc) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(inputPath),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(inputPath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        long dataIndex=0;
        int blockSize=0;
        List<Tuple2<Long,ArrayList<Double>>> dataSetPart=new ArrayList<Tuple2<Long,ArrayList<Double>>>();

        for(int i = 0; i < fileList.length; i++) {
            if (!fileList[i].isDirectory()) {
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
                while ((line = in.readLine()) != null) {
                    String[] tmpEle = line.substring(1,line.length()-1).split(",", 2);
                    Long dataID = Long.parseLong(tmpEle[0]);
                    ArrayList<Double> tmpElePro = new ElectricProfile(tmpEle[1]).getPoints();
                    dataSetPart.add(new Tuple2<Long, ArrayList<Double>>(dataID,tmpElePro));
                    blockSize++;
                    dataIndex++;
                    if (blockSize>=calculateBlockSize){
                        blockSize=0;
                        JavaPairRDD<Long,ArrayList<Double>> partData=sc.parallelizePairs(dataSetPart);
                        JavaRDD<Tuple3<Long,Long,Double>> tmpEdgeSet=createGraphEdgeSetV3(partData,dataSet,distCalculateFlag);
                        tmpEdgeSet.saveAsTextFile(HDFSOutputPath+"tmp/endWith-"+dataIndex+"/");
                        dataSetPart.clear();

                        System.out.println("******************************"+"  finish: "+dataIndex+"  **********************************");
                        System.out.println("******************************"+"  finish: "+dataIndex+"  **********************************");
                        System.out.println("******************************"+"  finish: "+dataIndex+"  **********************************");
                        System.out.println("******************************"+"  finish: "+dataIndex+"  **********************************");
                    }
                }
            }
        }

        if (dataSetPart.size()>0){
            JavaPairRDD<Long,ArrayList<Double>> partData=sc.parallelizePairs(dataSetPart);
            JavaRDD<Tuple3<Long,Long,Double>> tmpEdgeSet=createGraphEdgeSetV3(partData,dataSet,distCalculateFlag);
            tmpEdgeSet.saveAsTextFile(HDFSOutputPath+"tmp/endWith-"+dataIndex+"/");
            dataSetPart.clear();
        }

        class splitEdge implements PairFunction<String,Double,Tuple2<Long,Long>> {
            @Override
            public Tuple2<Double, Tuple2<Long, Long>> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",");
                return new Tuple2<Double, Tuple2<Long, Long>>(Double.parseDouble(tmpStr[2]),
                        new Tuple2<Long, Long>(Long.parseLong(tmpStr[0]),
                                Long.parseLong(tmpStr[1])));
            }
        }
        JavaRDD<String> lines=sc.textFile(HDFSOutputPath+"tmp/*/").distinct();
        JavaPairRDD<Double,Tuple2<Long,Long>> tmpValueRDD=lines.mapToPair(new splitEdge());
        tmpValueRDD.cache();
        List<Tuple2<Double,Tuple2<Long,Long>>> maxNumber=tmpValueRDD.sortByKey(false).take(1);
        final double maxValue=maxNumber.get(0)._1;

        class transToSimilar implements Function<Tuple2<Double,Tuple2<Long,Long>>,Tuple3<Long,Long,Double>> {
            @Override
            public Tuple3<Long, Long, Double> call(Tuple2<Double, Tuple2<Long, Long>> doubleTuple2Tuple2) throws Exception {
                return new Tuple3<Long, Long, Double>(
                        doubleTuple2Tuple2._2._1,
                        doubleTuple2Tuple2._2._2,
                        1-(doubleTuple2Tuple2._1/maxValue)
                );
            }
        }// 把表示距离的边转换成表示相似度权重的边

        tmpValueRDD.map(new transToSimilar()).saveAsTextFile(HDFSOutputPath+"result/");

        Path f = new Path(HDFSOutputPath+"tmp");
        FileSystem hdfs = FileSystem.get(URI.create(inputPath),new Configuration());
        boolean isExists = hdfs.exists(f);
        if (isExists) { //if exists, delete
            boolean isDel = hdfs.delete(f,true);
        }
        if (in!=null){
            in.close();
        }
        if (fsi!=null){
            fsi.close();
        }

    }


    public static JavaRDD<Tuple3<Long,Long,Double>> createGraphEdgeSetV3(JavaPairRDD<Long,ArrayList<Double>> partDataSet,JavaPairRDD<Long,ArrayList<Double>> dataSet,int distCalculateFlag){
        final int distFlag=distCalculateFlag;
        dataSet.cache();
        final int dataSetCountThr=(int) (dataSet.count()*0.05);

        class claculateDist implements PairFunction<Tuple2<Tuple2<Long,ArrayList<Double>>,Tuple2<Long,ArrayList<Double>>>,Long,Tuple2<Double,Long>>{
            @Override
            public Tuple2<Long, Tuple2<Double, Long>> call(Tuple2<Tuple2<Long, ArrayList<Double>>, Tuple2<Long, ArrayList<Double>>> tuple2Tuple2Tuple2) throws Exception {
                double tmpdist=Double.MAX_VALUE;
                if (distFlag==0){
                    Distance dist=new Distance();
                    tmpdist=dist.getDTWDistance(tuple2Tuple2Tuple2._1._2,tuple2Tuple2Tuple2._2._2);
                }else if(distFlag==1){
                    Tuple2<Integer,Double> tmp= FftConv.getMaxNNCc(tuple2Tuple2Tuple2._1._2,tuple2Tuple2Tuple2._2._2);
                    tmpdist=1-tmp._2;
                }else {
                    Distance dist=new Distance();
                    tmpdist= dist.getEuclideanDistance(tuple2Tuple2Tuple2._1._2,tuple2Tuple2Tuple2._2._2);
                }
                return new Tuple2<Long, Tuple2<Double, Long>>(tuple2Tuple2Tuple2._1._1,new Tuple2<Double, Long>(tmpdist,tuple2Tuple2Tuple2._2._1));
            }
        }
        class createSortKey implements PairFunction<Tuple2<Long,Tuple2<Double,Long>>,SortdKey,Tuple2<Long,Double>>{
            @Override
            public Tuple2<SortdKey, Tuple2<Long, Double>> call(Tuple2<Long, Tuple2<Double, Long>> longTuple2Tuple2) throws Exception {
                SortdKey tmpSK=new SortdKey(longTuple2Tuple2._1,longTuple2Tuple2._2._1);
                Tuple2<Long,Double> tmpData=new Tuple2<Long, Double>(longTuple2Tuple2._1,longTuple2Tuple2._2._1);
                return new Tuple2<SortdKey, Tuple2<Long, Double>>(tmpSK,tmpData);
            }
        }
        class getData implements PairFunction<Tuple2<SortdKey,Tuple2<Long,Double>>,Long,Double>{
            @Override
            public Tuple2<Long, Double> call(Tuple2<SortdKey, Tuple2<Long, Double>> sortdKeyTuple2Tuple2) throws Exception {
                return new Tuple2<Long, Double>(sortdKeyTuple2Tuple2._2._1,sortdKeyTuple2Tuple2._2._2);
            }
        }
        class getDistThr implements PairFunction<Tuple2<Long,Iterable<Double>>,Long,Double>{
            @Override
            public Tuple2<Long, Double> call(Tuple2<Long, Iterable<Double>> longIterableTuple2) throws Exception {
                int index=1;
                double tmpThr=-1;
                for (Double dataDist:longIterableTuple2._2
                        ) {
                    if (index>=dataSetCountThr){
                        tmpThr=dataDist*0.5;
                        break;
                    }
                    index++;
                }
                return new Tuple2<Long, Double>(longIterableTuple2._1,tmpThr);
            }
        }
        class filterForEdge implements Function<Tuple2<Long,Tuple2<Tuple2<Double,Long>,Double>>,Boolean>{
            @Override
            public Boolean call(Tuple2<Long, Tuple2<Tuple2<Double, Long>, Double>> longTuple2Tuple2) throws Exception {
                return (longTuple2Tuple2._2._1._1<=longTuple2Tuple2._2._2)&&(!longTuple2Tuple2._1.equals(longTuple2Tuple2._2._1._2));
            }
        }
        class getEdgeSet implements Function<Tuple2<Long,Tuple2<Tuple2<Double,Long>,Double>>,Tuple3<Long,Long,Double>>{
            @Override
            public Tuple3<Long, Long, Double> call(Tuple2<Long, Tuple2<Tuple2<Double, Long>, Double>> longTuple2Tuple2) throws Exception {
                Tuple3<Long,Long,Double> tmpEdge;
                if (longTuple2Tuple2._1<longTuple2Tuple2._2._1._2){
                    tmpEdge=new Tuple3<Long, Long, Double>(longTuple2Tuple2._1,longTuple2Tuple2._2._1._2,longTuple2Tuple2._2._1._1);
                }else {
                    tmpEdge=new Tuple3<Long, Long, Double>(longTuple2Tuple2._2._1._2,longTuple2Tuple2._1,longTuple2Tuple2._2._1._1);
                }
                return tmpEdge;
            }
        }

        //JavaPairRDD<Long,Tuple2<Double,Long>> cartDist=partDataSet.cartesian(dataSet).mapToPair(new claculateDist());
        JavaPairRDD<Long,Tuple2<Double,Long>> cartDist=partDataSet.cartesian(dataSet).mapToPair(new claculateDist());
        cartDist.cache();
        cartDist.count();

        JavaPairRDD<SortdKey,Tuple2<Long,Double>> sortRDD=cartDist.mapToPair(new createSortKey());
        sortRDD.cache();
        sortRDD.count();

        JavaPairRDD<SortdKey,Tuple2<Long,Double>> sortedRDD=sortRDD.sortByKey(false);
        sortedRDD.cache();
        sortedRDD.count();

        JavaPairRDD<Long,Double> dataThrSet=sortedRDD.mapToPair(new getData()).groupByKey().mapToPair(new getDistThr());
        dataThrSet.cache();
        dataThrSet.count();

        return  cartDist.join(dataThrSet).filter(new filterForEdge()).map(new getEdgeSet()).distinct();
    }

    public static void createGraph(String HDFSInputPath,String HDFSOutputPathVex,String HDFSOutputPathEdge,int distCalculateFlag,int calculateBlockSize,JavaSparkContext sc) throws IOException {

        JavaRDD<String> lines = sc.textFile(HDFSInputPath);
        class splitForOrigin implements PairFunction<String,Long,ArrayList<Double>>{
            @Override
            public Tuple2<Long, ArrayList<Double>> call(String s) throws Exception {
                String[] tmpEle=s.substring(1,s.length()-1).split(",",2);
                Long dataID=Long.parseLong(tmpEle[0]);
                ArrayList<Double> tmpElePro=new ElectricProfile(tmpEle[1]).getPoints();
                return new Tuple2<Long, ArrayList<Double>>(dataID,tmpElePro);
            }
        }
        JavaPairRDD<Long,ArrayList<Double>> markedDataSet=lines.mapToPair(new splitForOrigin());
        markedDataSet.saveAsTextFile(HDFSOutputPathVex);
        getEdgeSet(markedDataSet,HDFSInputPath,HDFSOutputPathEdge,distCalculateFlag,calculateBlockSize,sc);
    }


    public static void main(String[] args) throws IOException {

        //String master="spark://10.30.5.137:7077";
        //String jarPath="/home/hdp/IdeaProjects/ElectricityBigDataAnalysis/out/artifacts/ElectricityBigDataAnalysis_jar/ElectricityBigDataAnalysis.jar";
       // String inputPath="hdfs://10.30.5.137:9000/markedData/";
        //String[] jarPathArray={jarPath};
        //SparkConf conf=new SparkConf().setAppName("CreateEdge3V").setMaster(master).setJars(jarPathArray);

        //int flag=1;
        //int blockSize=15;

        String inputPath=args[0];
        int flag=Integer.parseInt(args[1]);
        int blockSize=Integer.parseInt(args[2]);

        //int flag=1;
        //int blockSize=10;
        SparkConf conf=new SparkConf().setAppName("CreateGraph");//.setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);

        String tmpIn=inputPath.trim();
        tmpIn=tmpIn.substring(0,tmpIn.length()-1);
        tmpIn=tmpIn+"-graph/";
        //String markedDataPath=tmpIn+"-marked/";
        //int flag=2;//0动态扭曲，1形状距离，2欧式距离
        String vexOutPath=tmpIn+"vexBy-"+flag+"/";
        String edgeOutPath=tmpIn+"edgeBy-"+flag+"/";

        //markDatatByID(inputPath,markedDataPath);
        createGraph(inputPath,vexOutPath,edgeOutPath,flag,blockSize,sc);
    }
}
