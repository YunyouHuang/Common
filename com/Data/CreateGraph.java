package com.Data;

import com.FileOperate.HDFSOperate;
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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 创建图的  边集合，顶点集合
 * Created by hdp on 16-9-16.
 */
public class CreateGraph {


    public static void getEdgeSet(JavaPairRDD<Long,ElectricProfile> dataSet,String inputPath,String HDFSOutputPath,int distCalculateFlag) throws IOException {

        JavaRDD<Tuple3<Long,Long,Double>> edgeSet=null;
        FileSystem fs = FileSystem.get(URI.create(inputPath),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(inputPath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        for(int i = 0; i < fileList.length; i++) {
            if (!fileList[i].isDirectory()) {
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
                while ((line = in.readLine()) != null) {
                    String[] tmpEle = line.split(",", 2);
                    Long dataID = Long.parseLong(tmpEle[0]);
                    ElectricProfile tmpElePro = new ElectricProfile(tmpEle[1]);
                    System.out.println("\n\n************************" + dataID + "**************************\n\n");
                    Tuple2<Long, ElectricProfile> data = new Tuple2<Long, ElectricProfile>(dataID, tmpElePro);
                    dataSet.cache();
                    JavaRDD<Tuple3<Long, Long, Double>> tmpEdgeSet = createGraphEdgeSetV2(data, dataSet,distCalculateFlag);
                    //List<Tuple3<Long, Long, Double>> dug=tmpEdgeSet.collect();
                    //long countDug=dug.size();
                    tmpEdgeSet.cache();
                    tmpEdgeSet.count();
                    if (edgeSet == null) {
                        edgeSet = tmpEdgeSet;
                    } else {
                        edgeSet = edgeSet.union(tmpEdgeSet).distinct();
                        //edgeSet.cache();
                    }
                }
            }
        }
        if (edgeSet != null) {
            //List<Tuple3<Long, Long, Double>> dug=edgeSet.collect();
            //long countDug=dug.size();
            edgeSet.coalesce(1, true).saveAsTextFile(HDFSOutputPath);
        }
        if (in!=null){
            in.close();
        }
        if (fsi!=null){
            fsi.close();
        }

    }

    public static JavaRDD<Tuple3<Long,Long,Double>> createGraphEdgeSetV2(Tuple2<Long,ElectricProfile> dataWithID,JavaPairRDD<Long,ElectricProfile> dataSet,int distCalculateFlag){

        final Tuple2<Long,ElectricProfile> data=dataWithID;

        class calculateDTW implements PairFunction<Tuple2<Long,ElectricProfile>,Double,Long>{
            @Override
            public Tuple2<Double, Long> call(Tuple2<Long,ElectricProfile> longElectricProfileTuple2) throws Exception {
                Long dataID=longElectricProfileTuple2._1;
                ElectricProfile tmpElePro=longElectricProfileTuple2._2;
                Distance dist=new Distance();
                double dtwDist=dist.getDTWDistance(data._2.getPoints(),tmpElePro.getPoints());
                return new Tuple2<Double,Long>(dtwDist,dataID);
            }
        }

        class calculateCC implements PairFunction<Tuple2<Long,ElectricProfile>,Double,Long>{
            @Override
            public Tuple2<Double, Long> call(Tuple2<Long, ElectricProfile> longElectricProfileTuple2) throws Exception {
                Long dataID=longElectricProfileTuple2._1;
                ElectricProfile tmpElePro=longElectricProfileTuple2._2;
                Tuple2<Integer,Double> tmp= FftConv.getMaxNNCc(data._2.getPoints(),tmpElePro.getPoints());
                return new Tuple2<Double, Long>(1-tmp._2,dataID);
            }
        }

        class calculateEUDist implements PairFunction<Tuple2<Long,ElectricProfile>,Double,Long>{
            @Override
            public Tuple2<Double, Long> call(Tuple2<Long, ElectricProfile> longElectricProfileTuple2) throws Exception {
                Long dataID=longElectricProfileTuple2._1;
                ElectricProfile tmpElePro=longElectricProfileTuple2._2;
                Distance dist=new Distance();
                double dtwDist= dist.getEuclideanDistance(data._2.getPoints(),tmpElePro.getPoints());
                return new Tuple2<Double,Long>(dtwDist,dataID);
            }
        }
        JavaPairRDD<Double,Long> dtwDataSet;

        if (distCalculateFlag==0){
            dtwDataSet=dataSet.mapToPair(new calculateDTW());
        }else if(distCalculateFlag==1){
            dtwDataSet=dataSet.mapToPair(new calculateCC());
        } else {
            dtwDataSet=dataSet.mapToPair(new calculateEUDist());
        }
        dtwDataSet.cache();
        int countDug=(int)(dtwDataSet.count()*0.05);
        JavaPairRDD<Double,Long> dtwDataSetWithOrder=dtwDataSet.sortByKey(false);



        List<Tuple2<Double,Long>> maxDistanceWhole=dtwDataSetWithOrder.collect();//按降序排序，取前面K个元素
        int tmpIdex=maxDistanceWhole.size();
        System.out.println("\n\n************************MAXDistan-----"+maxDistanceWhole.get(tmpIdex-1)._1+"**************************\n\n");
        System.out.println("\n\n************************MAXDistan-----"+maxDistanceWhole.get(tmpIdex-2)._1+"**************************\n\n");
        System.out.println("\n\n************************MAXDistan-----"+maxDistanceWhole.get(tmpIdex-3)._1+"**************************\n\n");

        List<Tuple2<Double,Long>> maxDistance=dtwDataSetWithOrder.take(countDug);//按降序排序，取前面K个元素
        System.out.println("\n\n************************MAXDistan-----"+maxDistance.get(0)._1+"**************************\n\n");
        System.out.println("\n\n************************MAXDistan-----"+maxDistance.get(1)._1+"**************************\n\n");
        System.out.println("\n\n************************MAXDistan-----"+maxDistance.get(countDug-2)._1+"**************************\n\n");
        System.out.println("\n\n************************MAXDistan-----"+maxDistance.get(countDug-1)._1+"**************************\n\n");

        //dataSet.cache();

        double intermediate=maxDistance.get(countDug-1)._1*0.82;
        final double dtwThr=intermediate;

        class filterDTW implements Function<Tuple2<Double,Long>,Boolean>{
            @Override
            public Boolean call(Tuple2<Double, Long> doubleLongTuple2) throws Exception {
                return (!doubleLongTuple2._2.equals(data._1))&&(doubleLongTuple2._1<=dtwThr);
            }
        }
        class getEdgeSet implements FlatMapFunction<Tuple2<Double,Long>, Tuple3<Long,Long,Double>>{
            @Override
            public Iterable<Tuple3<Long, Long, Double>> call(Tuple2<Double, Long> doubleLongTuple2) throws Exception {
                List<Tuple3<Long, Long, Double>> retValue=new ArrayList<Tuple3<Long, Long, Double>>();
                if(data._1<doubleLongTuple2._2){
                    retValue.add(new Tuple3<Long, Long, Double>(data._1,doubleLongTuple2._2,doubleLongTuple2._1));
                }else {
                    retValue.add(new Tuple3<Long, Long, Double>(doubleLongTuple2._2,data._1,doubleLongTuple2._1));
                }
                return retValue;
            }
        }
        JavaRDD<Tuple3<Long,Long,Double>> tmpEdgeSet=dtwDataSet.filter(new filterDTW()).flatMap(new getEdgeSet());
        return tmpEdgeSet;
    }


    public void createGraphEdgeSet(String master,String inputPath,String HDFSInputPath,String HDFSOutputPath){
        JavaRDD<Tuple3<Long,Long,Double>> edgeSet=null;
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(inputPath),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                String[] tmpData=line.split(",",2);
                ElectricProfile tmpProfile=new ElectricProfile(tmpData[1]);
                final Tuple2<Long,ElectricProfile> data=new Tuple2<Long, ElectricProfile>(Long.parseLong(tmpData[0]),tmpProfile);

                JavaSparkContext sc = new JavaSparkContext(master, "getEdgeSet"+": "+tmpData[0]);
                JavaRDD<String> dataSet = sc.textFile(HDFSInputPath);
                class calculateDTW implements PairFunction<String,Double,Long>{
                    @Override
                    public Tuple2<Double, Long> call(String s) throws Exception {
                        String[] tmpEle=s.split(",",2);
                        Long dataID=Long.parseLong(tmpEle[0]);
                        ElectricProfile tmpElePro=new ElectricProfile(tmpEle[1]);
                        Distance dist=new Distance();
                        double dtwDist=dist.getDTWDistance(data._2.getPoints(),tmpElePro.getPoints());
                        return new Tuple2<Double,Long>(new Double(dtwDist),dataID);
                    }
                }

                JavaPairRDD<Double,Long> dtwDataSet=dataSet.mapToPair(new calculateDTW());
                List<Tuple2<Double,Long>> maxDistance=dtwDataSet.top(1);
                double intermediate=0;
                for (Tuple2<Double, Long> maxDist :
                        maxDistance) {
                    intermediate = maxDist._1 / 2;
                    break;
                }

                final double dtwThr=intermediate;
                class filterDTW implements Function<Tuple2<Double,Long>,Boolean>{
                    @Override
                    public Boolean call(Tuple2<Double, Long> doubleLongTuple2) throws Exception {
                        return (!doubleLongTuple2._2.equals(data._1))&&(doubleLongTuple2._1<=dtwThr);
                    }
                }
                class getEdgeSet implements FlatMapFunction<Tuple2<Double,Long>, Tuple3<Long,Long,Double>>{
                    @Override
                    public Iterable<Tuple3<Long, Long, Double>> call(Tuple2<Double, Long> doubleLongTuple2) throws Exception {
                        List<Tuple3<Long, Long, Double>> retValue=new ArrayList<Tuple3<Long, Long, Double>>();
                        if(data._1<doubleLongTuple2._2){
                            retValue.add(new Tuple3<Long, Long, Double>(data._1,doubleLongTuple2._2,doubleLongTuple2._1));
                        }else {
                            retValue.add(new Tuple3<Long, Long, Double>(doubleLongTuple2._2,data._1,doubleLongTuple2._1));
                        }
                        return retValue;
                    }
                }

                JavaRDD<Tuple3<Long,Long,Double>> tmpEdgeSet=dtwDataSet.filter(new filterDTW()).flatMap(new getEdgeSet());
                if (edgeSet==null){
                    edgeSet=tmpEdgeSet;
                }else {
                    JavaRDD<Tuple3<Long,Long,Double>> unionEdgeSet=edgeSet.union(tmpEdgeSet);
                    edgeSet=unionEdgeSet.distinct();
                }

            }

            //把边的集合写到HDSF上
            if (edgeSet!=null){
                edgeSet.coalesce(1,true).saveAsTextFile(HDFSOutputPath);
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //创建节点集合，节点信息<节点ID，<客户ID，用电日期，数据类型>>
    public static void createGraphNodeSet(JavaPairRDD<Long,ElectricProfile> markedDataSet,String HDFSOutputPath){

        class getNodesSet implements Function<Tuple2<Long,ElectricProfile>,Tuple2<Long,Tuple3<Long,String,Integer>>>{
            @Override
            public Tuple2<Long,Tuple3<Long,String,Integer>> call(Tuple2<Long, ElectricProfile> longElectricProfileTuple2) throws Exception {
                return new Tuple2<Long,Tuple3<Long,String, Integer>>(longElectricProfileTuple2._1,
                                                                   new Tuple3<Long, String, Integer>(
                                                                           longElectricProfileTuple2._2.getID(),
                                                                           longElectricProfileTuple2._2.getDataDate(),
                                                                           longElectricProfileTuple2._2.getDataType()
                                                                   ));
            }
        }
        markedDataSet.map(new getNodesSet()).coalesce(1,true).saveAsTextFile(HDFSOutputPath);
    }

    public static void createGraph(String master,String jPath,String HDFSInputPath,String HDFSOutputPathVex,String HDFSOutputPathEdge,int distCalculateFlag) throws IOException {

        String[] jarPath={jPath};
        SparkConf conf=new SparkConf().setAppName("Encoding").setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(HDFSInputPath);//原始编码数据不用去除头尾的括号
        class splitForOrigin implements PairFunction<String,Long,ElectricProfile>{
            @Override
            public Tuple2<Long, ElectricProfile> call(String s) throws Exception {
                String[] tmpEle=s.split(",",2);
                Long dataID=Long.parseLong(tmpEle[0]);
                ElectricProfile tmpElePro=new ElectricProfile(tmpEle[1]);
                return new Tuple2<Long, ElectricProfile>(dataID,tmpElePro);
            }
        }
        JavaPairRDD<Long,ElectricProfile> markedDataSet=lines.mapToPair(new splitForOrigin());
        getEdgeSet(markedDataSet,HDFSInputPath,HDFSOutputPathEdge,distCalculateFlag);
        createGraphNodeSet(markedDataSet,HDFSOutputPathVex);
    }

    public static void markDatatByID(String inputPath,String outputPath) throws IOException {
        long index=0;
        long writCount=0;
        String tmpLine="";
        FileSystem fs = FileSystem.get(URI.create(inputPath),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(inputPath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        for(int i = 0; i < fileList.length; i++){
            if(!fileList[i].isDirectory()){
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi,"UTF-8"));
                while((line = in.readLine()) != null){
                    tmpLine+=index+","+line+"\n";
                    writCount++;
                    if (writCount>=8500){
                        writCount=0;
                        HDFSOperate.writeToHdfs(outputPath+"endWith-"+index+".txt",tmpLine);
                        tmpLine="";
                    }
                    index++;
                }
            }
        }
        if (!tmpLine.equals("")){
            HDFSOperate.writeToHdfs(outputPath+"endWith-"+(index-1),tmpLine);
            tmpLine="";
        }
        in.close();
        fsi.close();
    }

    public static void main(String[] args) throws IOException {
        String master="spark://10.30.5.137:7077";
        String jarPath="/home/hdp/IdeaProjects/ElectricityBigDataAnalysis/out/artifacts/ElectricityBigDataAnalysis_jar/ElectricityBigDataAnalysis.jar";
        String inputPath="hdfs://10.30.5.137:9000/spark/originData/test100-transBy-2/";
        String tmpIn=inputPath.trim();
        tmpIn=tmpIn.substring(0,tmpIn.length()-1);
        tmpIn=tmpIn+"-createGraph/";
        String markedDataPath=tmpIn+"marked/";
        int flag=2;
        String vexOutPath=tmpIn+"vexBy-"+flag+"/";
        String edgeOutPath=tmpIn+"edgeBy-"+flag+"/";

        markDatatByID(inputPath,markedDataPath);
        createGraph(master,jarPath,markedDataPath,vexOutPath,edgeOutPath,flag);
    }
}
