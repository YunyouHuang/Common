package com.Data;

import JavaTuple.Tupleid;
import com.Similarity.Distance;
import com.Transform.COMTrans;
import com.Transform.FFT;
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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hdp on 16-12-16.
 */
public class CreateGraph4V {

    public static void getEdgeSet(JavaPairRDD<Long,ArrayList<Float>> dataSet, String inputPath, String HDFSOutputPath,
                                  int distCalculateFlag, int calculateBlockSize, JavaSparkContext sc,
                                  double rate,double dataThr) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(inputPath),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(inputPath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        long dataIndex=0;
        int blockSize=0;
        List<Tuple2<Long,ArrayList<Float>>> dataSetPart=new ArrayList<Tuple2<Long,ArrayList<Float>>>();
        Map<Long,ArrayList<Float>> dataMap=new HashMap<Long, ArrayList<Float>>();
        dataSet.cache();
        for(int i = 0; i < fileList.length; i++) {
            if (!fileList[i].isDirectory()) {
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi, "UTF-8"));
                while ((line = in.readLine()) != null) {
                    String[] tmpEle = line.substring(1,line.length()-1).split(",", 2);
                    Long dataID = Long.parseLong(tmpEle[0]);
                    dataSetPart.add(new Tuple2<Long, ArrayList<Float>>(dataID,DataOperation.getFloatArray(tmpEle[1])));
                    dataMap.put(dataID,DataOperation.getFloatArray(tmpEle[1]));
                    blockSize++;
                    dataIndex++;
                    if (blockSize>=calculateBlockSize){
                        blockSize=0;
                        createGraphEdgeSetV3(sc,dataSetPart,dataSet,distCalculateFlag,rate,HDFSOutputPath+"tmp/endWith-"+dataIndex+"/",dataThr,dataMap);
                        dataSetPart.clear();
                        dataMap.clear();
                        System.out.println("******************************"+"  finish: "+dataIndex+"  **********************************");
                        System.out.println("******************************"+"  finish: "+dataIndex+"  **********************************");
                        System.out.println("******************************"+"  finish: "+dataIndex+"  **********************************");
                        System.out.println("******************************"+"  finish: "+dataIndex+"  **********************************");
                    }
                }
            }
        }

        if (dataSetPart.size()>0){
            createGraphEdgeSetV3(sc,dataSetPart,dataSet,distCalculateFlag,rate,HDFSOutputPath+"tmp/endWith-"+dataIndex+"/",dataThr,dataMap);
            dataSetPart.clear();
            dataMap.clear();
        }

        class splitEdge implements PairFunction<String,String,Double>{
            @Override
            public Tuple2<String, Double> call(String s) throws Exception {
                String[] tmpStr=s.substring(1,s.length()-1).split(",");
                return new Tuple2<String,Double>(tmpStr[0]+"-"+tmpStr[1],Double.parseDouble(tmpStr[2]));
            }
        }

        class reduceForDistinct implements Function2<Double,Double,Double>{
            @Override
            public Double call(Double aFloat, Double aFloat2) throws Exception {
                double retValue=aFloat;
                if (Math.random()>0.5){
                    retValue=aFloat2;
                }
                return retValue;
            }
        }

        class trans implements Function<Tuple2<String,Double>,Tuple3<Long,Long,Double>>{
            @Override
            public Tuple3<Long, Long, Double> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                String[] tmpStr=stringDoubleTuple2._1.split("-");
                return new Tuple3<Long, Long, Double>(Long.parseLong(tmpStr[0]),Long.parseLong(tmpStr[1]),stringDoubleTuple2._2);
            }
        }

        JavaRDD<String> lines=sc.textFile(HDFSOutputPath+"tmp/*/").coalesce(2000);
        if (distCalculateFlag==-1){
            lines.mapToPair(new splitEdge()).reduceByKey(new reduceForDistinct()).map(new trans()).saveAsTextFile(HDFSOutputPath+"result/");
        }else if (distCalculateFlag==1){
            JavaRDD<Tuple3<Long,Long,Double>> edgeSet=lines.mapToPair(new splitEdge()).reduceByKey(new reduceForDistinct()).map(new trans());
            edgeSet.saveAsTextFile(HDFSOutputPath+"result/");
        }else {
            class reduceForMax implements Function2<Tuple3<Long,Long,Double>,Tuple3<Long,Long,Double>,Tuple3<Long,Long,Double>>{
                @Override
                public Tuple3<Long, Long, Double> call(Tuple3<Long, Long, Double> longLongDoubleTuple3, Tuple3<Long, Long, Double> longLongDoubleTuple32) throws Exception {
                    double _max=longLongDoubleTuple3._3();
                    if (_max<longLongDoubleTuple32._3()){
                        _max=longLongDoubleTuple32._3();
                    }
                    return new Tuple3<Long, Long, Double>(-1l,-1l,_max);
                }
            }
            JavaRDD<Tuple3<Long,Long,Double>> edgeSet=lines.mapToPair(new splitEdge()).reduceByKey(new reduceForDistinct()).map(new trans());
            edgeSet.cache();
            final double _maxConsumption=edgeSet.reduce(new reduceForMax())._3();

            class norWithMax implements Function<Tuple3<Long,Long,Double>,Tuple3<Long,Long,Double>>{
                @Override
                public Tuple3<Long, Long, Double> call(Tuple3<Long, Long, Double> longLongDoubleTuple3) throws Exception {
                    return new Tuple3<Long, Long, Double>(longLongDoubleTuple3._1(),longLongDoubleTuple3._2(),1-(longLongDoubleTuple3._3()/(_maxConsumption*1.01)));
                }
            }

            edgeSet.map(new norWithMax()).saveAsTextFile(HDFSOutputPath+"result/");
        }

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


    public static void createGraphEdgeSetV3(JavaSparkContext sc, List<Tuple2<Long,ArrayList<Float>>> partDataSet,
                                            JavaPairRDD<Long,ArrayList<Float>> dataSet, int distCalculateFlag
                                                                         , double rate, String savePath, double thr, final Map<Long,ArrayList<Float>> dataMap){
        final int distFlag=distCalculateFlag;
        final float distRate=(float) rate;
        final float _fThr=(float) thr;

        final Broadcast<List<Tuple2<Long,ArrayList<Float>>>> broPartDataSet=sc.broadcast(partDataSet);

        class cartesianAndClaculateDist2V implements PairFlatMapFunction<Tuple2<Long,ArrayList<Float>>,Long,Tuple3<Long,Integer,Float>>{
            @Override
            public Iterable<Tuple2<Long, Tuple3<Long,Integer,Float>>> call(Tuple2<Long, ArrayList<Float>> longArrayListTuple2) throws Exception {
                List<Tuple2<Long, Tuple3<Long,Integer,Float>>> retValue=new ArrayList<Tuple2<Long, Tuple3<Long,Integer,Float>>>();
                List<Tuple2<Long,ArrayList<Float>>> partDataTable=broPartDataSet.getValue();
                for (Tuple2<Long,ArrayList<Float>> sigPartDataTable:partDataTable
                        ) {
                    double tmpdist=Double.MAX_VALUE;
                    if (distFlag==0){
                        Distance dist=new Distance();
                        tmpdist=dist.getDTWDistanceForFloat(sigPartDataTable._2,longArrayListTuple2._2);
                    }else if(distFlag==1){
                        Tupleid tmp= FFT.getSDB(DataOperation.Float2Doubel(sigPartDataTable._2),DataOperation.Float2Doubel(longArrayListTuple2._2));
                        tmpdist=1-tmp._2;
                    }else {
                        Distance dist=new Distance();
                        tmpdist= dist.getEuclideanDistanceForFloat(sigPartDataTable._2,longArrayListTuple2._2);
                    }
                    retValue.add(new Tuple2<Long, Tuple3<Long,Integer,Float>>(sigPartDataTable._1,new Tuple3<Long,Integer,Float>(longArrayListTuple2._1,1,(float)tmpdist)));
                }
                return retValue;
            }
        }

        class reduceForDist implements Function2<Tuple3<Long,Integer,Float>,Tuple3<Long,Integer,Float>,Tuple3<Long,Integer,Float>>{
            @Override
            public Tuple3<Long, Integer, Float> call(Tuple3<Long, Integer, Float> longIntegerFloatTuple3, Tuple3<Long, Integer, Float> longIntegerFloatTuple32) throws Exception {
                return new Tuple3<Long, Integer, Float>(-1l,longIntegerFloatTuple3._2()+longIntegerFloatTuple32._2(),longIntegerFloatTuple3._3()+longIntegerFloatTuple32._3());
            }
        }

        class getThr implements PairFunction<Tuple2<Long,Tuple3<Long,Integer,Float>>,Long,Float>{
            @Override
            public Tuple2<Long, Float> call(Tuple2<Long, Tuple3<Long, Integer, Float>> longTuple3Tuple2) throws Exception {
                return new Tuple2<Long, Float>(longTuple3Tuple2._1,longTuple3Tuple2._2._3()/longTuple3Tuple2._2._2()*distRate);
            }
        }

        JavaPairRDD<Long,Tuple3<Long,Integer,Float>> distDataSet=dataSet.flatMapToPair(new cartesianAndClaculateDist2V());
        distDataSet.cache();

        //distDataSet.saveAsTextFile(savePath.substring(0,savePath.length()-1)+"-Dist/");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");
        //System.out.println("********************************************************************");


        JavaPairRDD<Long,Float> thrDataSet=distDataSet.reduceByKey(new reduceForDist()).mapToPair(new getThr());
        //thrDataSet.cache();
        //thrDataSet.saveAsTextFile(savePath.substring(0,savePath.length()-1)+"-Thr/");

        final Broadcast<Map<Long,Float>> broThr=sc.broadcast(thrDataSet.collectAsMap());

        final Broadcast<Map<Long,ArrayList<Float>>> _dataMap=sc.broadcast(dataMap);

        broPartDataSet.unpersist();

        class fliterEdge implements Function<Tuple2<Long,Tuple3<Long,Integer,Float>>,Boolean>{
            @Override
            public Boolean call(Tuple2<Long, Tuple3<Long, Integer, Float>> longTuple3Tuple2) throws Exception {
                Map<Long,Float> thr=broThr.getValue();
                Map<Long,ArrayList<Float>> tmpDataMap=_dataMap.getValue();
                float _thr=thr.get(longTuple3Tuple2._1);
                ArrayList<Float> data=tmpDataMap.get(longTuple3Tuple2._1);
                boolean _near;
                if (distFlag==0){
                    //_near=longTuple3Tuple2._2._3()<(DataOperation.getSumForFloat(data)/(2*data.size())*_fThr);
                    _near=true;
                }else if (distFlag==1){
                    _near=(1-longTuple3Tuple2._2._3())<_fThr;
                }else {
                    _near=longTuple3Tuple2._2._3()<(Math.sqrt(Distance.getPowDistanceForFloat(data,new ArrayList<Float>()))*_fThr);
                }
                return _near&&(longTuple3Tuple2._2._3()<_thr)&&(!longTuple3Tuple2._1().equals(longTuple3Tuple2._2._1()));
            }
        }

        class getEdge implements Function<Tuple2<Long,Tuple3<Long,Integer,Float>>,Tuple3<Long,Long,Float>>{
            @Override
            public Tuple3<Long, Long, Float> call(Tuple2<Long, Tuple3<Long, Integer, Float>> longTuple3Tuple2) throws Exception {
                Tuple3<Long,Long,Float> tmpEdge;
                float _weight;
                if (distFlag==1){
                    _weight=1-longTuple3Tuple2._2._3();
                }else {
                    //_weight=(float) Math.exp(-0.5*longTuple3Tuple2._2._3());
                    _weight=longTuple3Tuple2._2()._3();
                }
                if (longTuple3Tuple2._1()<longTuple3Tuple2._2._1()){
                    tmpEdge=new Tuple3<Long, Long, Float>(longTuple3Tuple2._1,longTuple3Tuple2._2._1(),_weight);
                }else {
                    tmpEdge=new Tuple3<Long, Long, Float>(longTuple3Tuple2._2._1(),longTuple3Tuple2._1,_weight);
                }
                return tmpEdge;
            }
        }

        distDataSet.filter(new fliterEdge()).map(new getEdge()).distinct().saveAsTextFile(savePath);
        broThr.unpersist();
        distDataSet.unpersist();
        _dataMap.unpersist();
    }

    public static void createGraph(String HDFSInputPath,String HDFSOutputPathVex,String HDFSOutputPathEdge,
                                   int distCalculateFlag,int calculateBlockSize,JavaSparkContext sc,double rate,double dataThr) throws IOException {

        JavaRDD<String> lines = sc.textFile(HDFSInputPath);
        class splitForOrigin implements PairFunction<String,Long,ArrayList<Float>>{
            @Override
            public Tuple2<Long, ArrayList<Float>> call(String s) throws Exception {
                String[] tmpEle=s.substring(1,s.length()-1).split(",",2);
                Long dataID=Long.parseLong(tmpEle[0]);
                ArrayList<Float> tmpElePro=DataOperation.getFloatArray(tmpEle[1]);
                return new Tuple2<Long, ArrayList<Float>>(dataID,tmpElePro);
            }
        }
        JavaPairRDD<Long,ArrayList<Float>> markedDataSet=lines.mapToPair(new splitForOrigin());//.repartition(170);
        markedDataSet.saveAsTextFile(HDFSOutputPathVex);
        getEdgeSet(markedDataSet,HDFSInputPath,HDFSOutputPathEdge,distCalculateFlag,calculateBlockSize,sc,rate,dataThr);
    }


    public static void norAndMark(String input, String output, JavaSparkContext sc, int norFlag){
        JavaRDD<String> lines = sc.textFile(input).repartition(216);
        final int _norFlag=norFlag;
        class norbyZScore implements PairFunction<Tuple2<String,Long>,Long,ElectricProfile>{
            @Override
            public Tuple2<Long, ElectricProfile> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                COMTrans ct=new COMTrans();
                ElectricProfile tmpELE=new ElectricProfile(stringLongTuple2._1);
                ArrayList<Double> tmpPoint=tmpELE.getPoints();
                tmpELE.setPoints(ct.zscore(tmpPoint));
                return new Tuple2<Long, ElectricProfile>(stringLongTuple2._2(),tmpELE);
            }
        }

        class norbySumOrMax implements PairFunction<Tuple2<String,Long>,Long,ElectricProfile>{
            @Override
            public Tuple2<Long, ElectricProfile> call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                COMTrans ct=new COMTrans();
                ElectricProfile tmpELE=new ElectricProfile(stringLongTuple2._1);
                ArrayList<Double> tmpPoint=tmpELE.getPoints();
                tmpELE.setPoints(ct.normalizing(_norFlag,tmpPoint));
                return new Tuple2<Long, ElectricProfile>(stringLongTuple2._2(),tmpELE);
            }
        }
        if (norFlag<2){
            lines.zipWithUniqueId().mapToPair(new norbySumOrMax()).saveAsTextFile(output);
        }else {
            lines.zipWithUniqueId().mapToPair(new norbyZScore()).saveAsTextFile(output);
        }

    }


    public static void main(String[] args) throws IOException {

        String inputPath=args[0];
        int flag=Integer.parseInt(args[1]);
        int blockSize=Integer.parseInt(args[2]);
        String norSavePath=args[3];
        double rate=Double.parseDouble(args[4]);
        int isNor=Integer.parseInt(args[5]);
        double dataThr=Double.parseDouble(args[6]);
        int norFlag=Integer.parseInt(args[7]);
        SparkConf conf=new SparkConf().setAppName("CreateGraph");//.setMaster(master).setJars(jarPathArray);
        JavaSparkContext sc = new JavaSparkContext(conf);

        String dataInput;
        if (isNor==0){
            norAndMark(inputPath,norSavePath,sc,norFlag);
            dataInput=norSavePath;
        }else {
            dataInput=inputPath;
        }
        String tmpIn=dataInput.trim();
        tmpIn=tmpIn+"-graphWithDist-"+flag+"-"+rate+"-"+dataThr+"/";
        //String markedDataPath=tmpIn+"-marked/";
        //int flag=2;//0动态扭曲，1形状距离，2欧式距离
        String vexOutPath=tmpIn+"node/";
        String edgeOutPath=tmpIn+"edge/";

        //markDatatByID(inputPath,markedDataPath);
        createGraph(dataInput,vexOutPath,edgeOutPath,flag,blockSize,sc,rate,dataThr);
    }
}
