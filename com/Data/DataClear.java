package com.Data;

import SparkClass.SplitPairELEWithMarkID;
import com.FileOperate.HDFSOperate;
import com.Similarity.Distance;
import com.Sort.InsertSort;
import com.Transform.FftConv;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.math.BigDecimal;
/**
 * Created by hdp on 17-2-20.
 */
public class DataClear implements Serializable{


    public JavaRDD<ElectricProfile> getPower(JavaRDD<ElectricProfile> dataSet,double thr){

        final  double _thr=thr;
        class getSortKey implements PairFunction<ElectricProfile,SortdKey,ElectricProfile>{
            @Override
            public Tuple2<SortdKey, ElectricProfile> call(ElectricProfile electricProfile) throws Exception {
                return new Tuple2<SortdKey,ElectricProfile>(new SortdKey(electricProfile.getID(),Double.parseDouble(electricProfile.getDataDate())),electricProfile);
            }
        }

        class getEleWithID implements PairFunction<Tuple2<SortdKey,ElectricProfile>,Long,ElectricProfile>{
            @Override
            public Tuple2<Long, ElectricProfile> call(Tuple2<SortdKey, ElectricProfile> sortdKeyElectricProfileTuple2) throws Exception {
                return new Tuple2<Long,ElectricProfile>(sortdKeyElectricProfileTuple2._1.getFirst(),sortdKeyElectricProfileTuple2._2);
            }
        }

        class getPowerData implements FlatMapFunction<Tuple2<Long,Iterable<ElectricProfile>>,ElectricProfile>{
            @Override
            public Iterable<ElectricProfile> call(Tuple2<Long, Iterable<ElectricProfile>> longIterableTuple2) throws Exception {
                return getPData2v(longIterableTuple2._2,_thr);
            }
        }

        return dataSet.mapToPair(new getSortKey()).sortByKey().mapToPair(new getEleWithID()).groupByKey().flatMap(new getPowerData());

    }


    private ArrayList<ElectricProfile> getPData2v(Iterable<ElectricProfile> originData,double thr){

        double preRead=-1;
        int sDay=-1;
        ArrayList<ElectricProfile> retVal=new ArrayList<ElectricProfile>();
        double userConsumption=0;
        for (ElectricProfile ele: InsertSort.sorForEleAscend(originData)
                ) {
            ElectricProfile tmpData=new ElectricProfile(ele.toString());
            ArrayList<Double> tmpPiont=new ArrayList<Double>();
            ArrayList<Double> readPiont=ele.getPoints();

            if (sDay==-1){
                sDay=Integer.parseInt(ele.getDataDate());
            }
            if (sDay<Integer.parseInt(ele.getDataDate())){
                sDay=Integer.parseInt(ele.getDataDate());
                preRead=-1;
            }
            if (sDay==Integer.parseInt(ele.getDataDate())){
                if (preRead==-1){
                    BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(1)));
                    BigDecimal tmp0=new BigDecimal(String.valueOf(readPiont.get(0)));
                    double tmpValue=tmp1.subtract(tmp0).doubleValue();
                    //double tmpValue=readPiont.get(1)-readPiont.get(0);
                    tmpPiont.add(tmpValue);
                }else {
                    BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(1)));
                    BigDecimal tmp0=new BigDecimal(String.valueOf(preRead));
                    double tmpValue=tmp1.subtract(tmp0).doubleValue();
                    tmpPiont.add(tmpValue);

                }
                for (int i = 0; i < readPiont.size()-1; i++) {
                    BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(i+1)));
                    BigDecimal tmp0=new BigDecimal(String.valueOf(readPiont.get(i)));
                    double tmpValue=tmp1.subtract(tmp0).doubleValue();
                    if (tmpValue>0){
                        userConsumption+=tmpValue;
                    }
                    if ((readPiont.get(i+1)<=0)&&(userConsumption>0)){
                        tmpPiont.add(-1.0);
                    }else {
                        tmpPiont.add(tmpValue);
                    }
                    preRead=readPiont.get(i+1);
                }
                if (isLegal(tmpPiont,thr)){
                    tmpData.setPoints(tmpPiont);
                    retVal.add(tmpData);
                }
                sDay=getNewDate(String.valueOf(sDay),1);
            }
        }

        return retVal;
    }

    public boolean isLegal(ArrayList<Double> retValue,double thr){
        double sum=0;
        for (int i = 0; i < retValue.size(); i++) {
            if (retValue.get(i)<0){
                return false;
            }else {
                sum+=retValue.get(i);
            }
        }
        double valueIndex0AndIndex1=retValue.get(0)+retValue.get(1);
        if (valueIndex0AndIndex1/(sum+0.0001)>thr){
            return false;
        }
        for (int i = 0; i < retValue.size(); i++) {
            double tmpValue=retValue.get(i);
            if (tmpValue/(sum+0.0001)>thr){
                return false;
            }
        }

        return true;
    }

    private ArrayList<ElectricProfile> getPData(Iterable<ElectricProfile> originData){

        double preRead=-1;
        int sDay=-1;
        ArrayList<ElectricProfile> retVal=new ArrayList<ElectricProfile>();

        for (ElectricProfile ele: InsertSort.sorForEleAscend(originData)
             ) {
            ElectricProfile tmpData=new ElectricProfile(ele.toString());
            ArrayList<Double> tmpPiont=new ArrayList<Double>();
            ArrayList<Double> readPiont=ele.getPoints();

            if (sDay==-1){
                sDay=Integer.parseInt(ele.getDataDate());
            }
            if (sDay<Integer.parseInt(ele.getDataDate())){
                sDay=Integer.parseInt(ele.getDataDate());
                preRead=-1;
            }
            if (sDay==Integer.parseInt(ele.getDataDate())){
                if (preRead==-1){
                    BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(1)));
                    BigDecimal tmp0=new BigDecimal(String.valueOf(readPiont.get(0)));
                    double tmpValue=tmp1.subtract(tmp0).doubleValue();
                    //double tmpValue=readPiont.get(1)-readPiont.get(0);
                    tmpPiont.add(tmpValue);
                }else {
                    if (preRead>readPiont.get(0)){
                        BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(1)));
                        BigDecimal tmp0=new BigDecimal(String.valueOf(readPiont.get(0)));
                        double tmpValue=tmp1.subtract(tmp0).doubleValue();
                        //double tmpValue=readPiont.get(1)-readPiont.get(0);
                        tmpPiont.add(tmpValue);
                    }else {
                        if (preRead!=0){
                            if (readPiont.get(0)/preRead>10){
                                BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(1)));
                                BigDecimal tmp0=new BigDecimal(String.valueOf(readPiont.get(0)));
                                double tmpValue=tmp1.subtract(tmp0).doubleValue();
                                //double tmpValue=readPiont.get(1)-readPiont.get(0);
                                tmpPiont.add(tmpValue);
                            }else {
                                BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(0)));
                                BigDecimal tmp0=new BigDecimal(String.valueOf(preRead));
                                double tmpValuePre =tmp1.subtract(tmp0).doubleValue();
                                //double tmpValuePre=readPiont.get(0)-preRead;
                                tmpPiont.add(tmpValuePre);
                            }
                        }else {
                            BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(0)));
                            BigDecimal tmp0=new BigDecimal(String.valueOf(preRead));
                            double tmpValuePre =tmp1.subtract(tmp0).doubleValue();
                            //double tmpValuePre=readPiont.get(0)-preRead;
                            tmpPiont.add(tmpValuePre);
                        }
                    }
                }
                for (int i = 0; i < readPiont.size()-1; i++) {
                    BigDecimal tmp1=new BigDecimal(String.valueOf(readPiont.get(i+1)));
                    BigDecimal tmp0=new BigDecimal(String.valueOf(readPiont.get(i)));
                    double tmpValue=tmp1.subtract(tmp0).doubleValue();
                    //double tmpValue=readPiont.get(i+1)-readPiont.get(i);
                    tmpPiont.add(tmpValue);
                    preRead=readPiont.get(i+1);
                }
                tmpData.setPoints(smoothLoad(tmpPiont));
                retVal.add(tmpData);
                sDay=getNewDate(String.valueOf(sDay),1);
            }
        }

        return retVal;
    }


    public ArrayList<Double> smoothLoad(ArrayList<Double> data){
        ArrayList<Double> retValue=new ArrayList<Double>();
        double sum=0;
        for (int i = 0; i < data.size(); i++) {
            if (data.get(i)<0){
                //data.set(i,-data.get(i));
                retValue.add(-data.get(i));
                sum+=-data.get(i);
            }else {
                retValue.add(data.get(i));
                sum+=data.get(i);
            }
        }
        double valueIndex0AndIndex1=retValue.get(0)+retValue.get(1);
        if (valueIndex0AndIndex1/sum>0.9){
            retValue.set(0,retValue.get(2));
            retValue.set(1,retValue.get(2));
            sum=sum-valueIndex0AndIndex1+2*retValue.get(2);
        }
        for (int i = 0; i < retValue.size(); i++) {
            double tmpValue=retValue.get(i);
            if (tmpValue/(sum+0.0001)>0.85){
                int index1=i-1;
                int index2=i+1;
                BigDecimal tmp1=null;
                BigDecimal tmp2=null;
                if (index1>0){
                    tmp1=new BigDecimal(String.valueOf(retValue.get(index1)));
                }
                if (index2<retValue.size()){
                    tmp2=new BigDecimal(String.valueOf(retValue.get(index2)));
                }
                double _vlaue=0;
                if ((tmp1!=null)&&(tmp2!=null)){
                    _vlaue=tmp1.doubleValue();
                }else if ((tmp1==null)&&(tmp2!=null)){
                    _vlaue=tmp2.doubleValue();
                }else if ((tmp1!=null)&&(tmp2==null)){
                    _vlaue=tmp1.doubleValue();
                }
                retValue.set(i,_vlaue);
            }
        }

        return retValue;
    }


    public JavaPairRDD<Integer,ElectricProfile> remapDataSet(HashMap<Integer,ElectricProfile> mergeCenters,JavaPairRDD<Integer,ElectricProfile> DataSet,String pcSavePath) throws IOException {
        ArrayList<ArrayList<Double>> centers=new ArrayList<ArrayList<Double>>();
        ArrayList<Integer> clusterMark=new ArrayList<Integer>();
        Iterator iter = mergeCenters.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<Integer,ElectricProfile> entry = (Map.Entry<Integer,ElectricProfile>) iter.next();
            Integer key = entry.getKey();
            ElectricProfile val = entry.getValue();
            if (val!=null){
                clusterMark.add(key);
                centers.add(val.getPoints());
            }
        }

        final ArrayList<Integer> clusterMapNum=clusterMark;
        class mapDatSet implements PairFunction<Tuple2<Integer,ElectricProfile>,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                Integer mapIndex=-1;
                for (int i = 0; i < clusterMapNum.size(); i++) {
                    if (integerElectricProfileTuple2._1().equals(clusterMapNum.get(i))){
                        mapIndex=i;
                        break;
                    }
                }
                return new Tuple2<Integer, ElectricProfile>(mapIndex,integerElectricProfileTuple2._2);
            }
        }

        String centerStr="";
        for (int i = 0; i < centers.size(); i++) {
            centerStr+=i;
            for (int j = 0; j < centers.get(i).size(); j++) {
                centerStr+=","+centers.get(i).get(j);
            }
            centerStr+="\n";
        }
        HDFSOperate.writeToHdfs(pcSavePath+"/centers.txt", centerStr);

        return DataSet.mapToPair(new mapDatSet());

    }

    public void clearDataSet(String dayStart, String dayEnd, ArrayList<Integer> delDate, JavaPairRDD<Integer,ElectricProfile> dataSet, String savePath, int calFlag){
        if (delDate.size()>0){
            if ((delDate.size()%2)!=0){
                System.out.print("******************** delete date illegal ! ************************");
                return ;
            }
        }
        final ArrayList<Integer> dateSet=delDate;
        final String startDay=dayStart;
        final String endDay=dayEnd;
        final int calDistFlag=calFlag;
        class filterDate implements Function<Tuple2<Integer,ElectricProfile>,Boolean>{
            @Override
            public Boolean call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                boolean flag=true;
                int eleDate=Integer.parseInt(integerElectricProfileTuple2._2.getDataDate());

                if ((eleDate>Integer.parseInt(endDay))||(eleDate<Integer.parseInt(startDay))){
                    return  false;
                }

                for (int i = 0; i <dateSet.size()/2 ; i++) {
                    int index=i*2;

                    if ((eleDate>=dateSet.get(index))&&(eleDate<=dateSet.get(index+1))){
                        return false;
                    }
                }
                return flag;
            }
        }

        class filterDelData implements Function<Tuple2<Long,Tuple2<Integer,ElectricProfile>>,Boolean>{
            @Override
            public Boolean call(Tuple2<Long, Tuple2<Integer, ElectricProfile>> longTuple2Tuple2) throws Exception {
                return !longTuple2Tuple2._2._1.equals(-1);
            }
        }

        class getSortKey implements PairFunction<Tuple2<Integer,ElectricProfile>,SortdKey,Tuple2<Integer,ElectricProfile>>{
            @Override
            public Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>> call(Tuple2<Integer, ElectricProfile> integerElectricProfileTuple2) throws Exception {
                return new Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>>(new SortdKey(integerElectricProfileTuple2._2.getID(),Double.parseDouble(integerElectricProfileTuple2._2.getDataDate())),integerElectricProfileTuple2);
            }
        }

        class getData implements PairFunction<Tuple2<SortdKey,Tuple2<Integer,ElectricProfile>>,Long,Tuple2<Integer,ElectricProfile>>{
            @Override
            public Tuple2<Long, Tuple2<Integer, ElectricProfile>> call(Tuple2<SortdKey, Tuple2<Integer, ElectricProfile>> sortdKeyTuple2Tuple2) throws Exception {

                return new Tuple2<Long, Tuple2<Integer, ElectricProfile>>(sortdKeyTuple2Tuple2._1.getFirst(),sortdKeyTuple2Tuple2._2);
            }
        }

        class getWeekDateSet implements PairFlatMapFunction<Tuple2<Long,Iterable<Tuple2<Integer,ElectricProfile>>>,Long,Tuple2<Integer,ElectricProfile>>{
            @Override
            public Iterable<Tuple2<Long, Tuple2<Integer, ElectricProfile>>> call(Tuple2<Long, Iterable<Tuple2<Integer, ElectricProfile>>> longIterableTuple2) throws Exception {

                long userID=-1;
                for (Tuple2<Integer, ElectricProfile> data:longIterableTuple2._2
                     ) {
                    userID=data._2.getID();
                    break;
                }
                List<Tuple2<Long,Tuple2<Integer,ElectricProfile>>> retVal=new ArrayList<Tuple2<Long, Tuple2<Integer,ElectricProfile>>>();
                ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> weekData=getCompleteDate(decomposttionDataSet(longIterableTuple2._2,startDay,endDay),dateSet,calDistFlag);
                //ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> weekData=decomposttionDataSet(longIterableTuple2._2,startDay,endDay);

                for (int i = 0; i < weekData.size(); i++) {
                    for (int j = 0; j < weekData.get(j).size(); j++) {
                        Tuple2<Long,Tuple2<Integer,ElectricProfile>> tmpTupl=new Tuple2<Long, Tuple2<Integer, ElectricProfile>>(
                                userID,weekData.get(i).get(j)
                        );
                        retVal.add(tmpTupl);
                    }
                }
                return retVal;
            }
        }

        class getEle implements Function<Tuple2<Long,Tuple2<Integer,ElectricProfile>>,ElectricProfile>{
            @Override
            public ElectricProfile call(Tuple2<Long, Tuple2<Integer, ElectricProfile>> longTuple2Tuple2) throws Exception {
                return longTuple2Tuple2._2._2;
            }
        }


        JavaPairRDD<Long,Tuple2<Integer,ElectricProfile>> comDataSet=dataSet.filter(new filterDate()).mapToPair(new getSortKey())
                .sortByKey().mapToPair(new getData()).groupByKey()
                .flatMapToPair(new getWeekDateSet());
                comDataSet.cache();
        //comDataSet.saveAsTextFile(savePath+"-tmpEX/");
        comDataSet.filter(new filterDelData()).map(new getEle())
               .saveAsTextFile(savePath+"complete/");
    }

    public ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> decomposttionDataSet(Iterable<Tuple2<Integer,ElectricProfile>> dailyData,String dayStart,String dayEnd){

        int startDate=Integer.parseInt(dayStart);
        int endDate=Integer.parseInt(dayEnd);


        ArrayList<Tuple2<Integer,ElectricProfile>> newDataSet=new ArrayList<Tuple2<Integer, ElectricProfile>>();
        for (Tuple2<Integer,ElectricProfile> tmpData:InsertSort.sortForEleWithMarkAscend(dailyData)
             ) {
            int curDate=Integer.parseInt(tmpData._2().getDataDate());
            while (startDate<curDate){
                ElectricProfile tmpEle=new ElectricProfile();
                tmpEle.setDataDate(String.valueOf(startDate));
                newDataSet.add(new Tuple2<Integer, ElectricProfile>(-1,tmpEle));
                System.out.print(startDate+"\n");
                startDate=getNewDate(String.valueOf(startDate),1);
            }
            newDataSet.add(tmpData);
            System.out.print(startDate+"\n");
            startDate=getNewDate(String.valueOf(startDate),1);
        }

        while (startDate<=endDate){
            ElectricProfile tmpEle=new ElectricProfile();
            tmpEle.setDataDate(String.valueOf(startDate));
            newDataSet.add(new Tuple2<Integer, ElectricProfile>(-1,tmpEle));
            System.out.print(startDate+"\n");
            startDate=getNewDate(String.valueOf(startDate),1);
        }

        ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> weekData=new ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>>();

        for (int i = 0; i < newDataSet.size()/7; i++) {
            ArrayList<Tuple2<Integer,ElectricProfile>> tmpWeek=new ArrayList<Tuple2<Integer, ElectricProfile>>();
            tmpWeek.add(newDataSet.get(7*i));
            tmpWeek.add(newDataSet.get(7*i+1));
            tmpWeek.add(newDataSet.get(7*i+2));
            tmpWeek.add(newDataSet.get(7*i+3));
            tmpWeek.add(newDataSet.get(7*i+4));
            tmpWeek.add(newDataSet.get(7*i+5));
            tmpWeek.add(newDataSet.get(7*i+6));
            weekData.add(tmpWeek);
        }

        return weekData;

    }

    //region 暂时不用 把日数据集转换为周数据集
    /*
    public ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> decompositionData(Iterable<Tuple2<Integer,ElectricProfile>> dailyData,String dayStart){
        ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> retVal=new ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>>();
        ArrayList<Tuple2<Integer,ElectricProfile>> tmpWeek=new ArrayList<Tuple2<Integer, ElectricProfile>>();
        int startYear=Integer.parseInt(dayStart.substring(0,4));
        int startMonth=Integer.parseInt(dayStart.substring(4,6));
        int startDay=Integer.parseInt(dayStart.substring(6,8));
        int addDaysNum=0;

        for (Tuple2<Integer,ElectricProfile> data:dailyData
             ) {
            String eleDate=data._2.getDataDate();
            int curYear=Integer.parseInt(eleDate.substring(0,4));
            int curMonth=Integer.parseInt(eleDate.substring(4,6));
            int curDay=Integer.parseInt(eleDate.substring(6,8));

            boolean addMonth=false;
            boolean addYear=false;
            boolean stopFlag=false;
            boolean addEleFlag=false;

            while (!stopFlag){
                int tmpYear=startYear;
                int tmpMonth=startMonth;
                int tmpDay=startDay+addDaysNum;

                int secondMonthDays=28;

                if ((tmpYear%100)==0){
                    if ((tmpYear%400)==0){
                        secondMonthDays=29;
                    }
                }else {
                    if ((tmpYear%4)==0){
                        secondMonthDays=29;
                    }
                }

                if ((tmpDay>secondMonthDays)&&(tmpMonth==2)){
                    tmpMonth=3;
                    tmpDay=tmpDay-secondMonthDays;
                }else if ((tmpDay>30)
                        &&(tmpMonth==4)
                        &&(tmpMonth==6)
                        &&(tmpMonth==9)
                        &&(tmpMonth==11)){
                    tmpMonth=tmpMonth+1;
                    tmpDay=tmpDay-30;
                }else if ((tmpDay>31)
                        &&(tmpMonth==1)
                        &&(tmpMonth==3)
                        &&(tmpMonth==5)
                        &&(tmpMonth==7)
                        &&(tmpMonth==8)
                        &&(tmpMonth==10)
                        &&(tmpMonth==12)
                        ){
                    if (tmpMonth==12){
                        tmpMonth=1;
                        tmpYear=tmpYear+1;
                    }else {
                        tmpMonth=tmpMonth+1;
                    }
                    tmpDay=tmpDay-31;
                }

                if (addDaysNum==7){
                    startYear=tmpYear;
                    startMonth=tmpMonth;
                    startDay=tmpDay;
                    addDaysNum=0;
                    ArrayList<Tuple2<Integer,ElectricProfile>> tmpEle=new ArrayList<Tuple2<Integer, ElectricProfile>>();
                    for (int i = 0; i < tmpWeek.size(); i++) {
                        tmpEle.add(tmpWeek.get(i));
                    }
                    retVal.add(tmpEle);
                    tmpWeek.clear();
                }

                if (addEleFlag){
                    break;
                }
                if ((tmpYear==curYear)&&(tmpMonth==curMonth)&&(tmpDay==curDay)){
                    tmpWeek.add(data);
                    addDaysNum++;
                    addEleFlag=true;
                }else {
                    ElectricProfile nullElectricProfile=new ElectricProfile();
                    String dateStr="";
                    dateStr+=tmpYear;
                    if (tmpMonth<10){
                        dateStr+="0"+tmpMonth;
                    }else {
                        dateStr+=tmpMonth;
                    }
                    if (tmpDay<10){
                        dateStr+="0"+tmpDay;
                    }else {
                        dateStr+=tmpDay;
                    }
                    nullElectricProfile.setDataDate(dateStr);
                    tmpWeek.add(new Tuple2<Integer, ElectricProfile>(-1,nullElectricProfile));
                    addDaysNum++;
                }
            }
        }
        return retVal;
    }
*/
    //endregion


    public ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> getCompleteDate(ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> dateSet,ArrayList<Integer> delDate,int calFlag){

        boolean isCompleteForYear=true;//一整年内是否有至少一个完整的周
        ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> retValu=new ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>>();

        for (int i = 0; i < dateSet.size(); i++) {
            ArrayList<Tuple2<Integer,ElectricProfile>> tmpWeekData=dateSet.get(i);
            if ((!isDel(delDate,tmpWeekData))&&(!isComplete(tmpWeekData))){

                int startIndex=-1;
                int endIndex=-1;

                int mid1=-1;
                int mid2=-1;
                double mixDist=Double.MAX_VALUE;
                ArrayList<Integer> bestIndex=new ArrayList<Integer>();

                boolean finish=false;

                //region  选择离自己最近最相似的周用电模式填充缺失的数据。
                for (int j = 0; j < 20; j++) {
                    startIndex=i-(3+j*2);
                    endIndex=i+(3+j*2);

                    if (startIndex<0){
                        startIndex=0;
                    }
                    if (endIndex>(dateSet.size()-1)){
                        endIndex=dateSet.size()-1;
                    }

                    if ((mid1==-1)&&(mid2==-1)){
                        for (int k = startIndex; k <endIndex+1 ; k++) {
                            if (k!=i){
                                ArrayList<Tuple2<Integer,ElectricProfile>> compareDate=dateSet.get(k);
                                if ((!isDel(delDate,compareDate))&&isComplete(compareDate)){
                                    finish=true;
                                    double tmpDist=getWeekDist(tmpWeekData,compareDate,calFlag);
                                    if (tmpDist<mixDist){
                                        bestIndex.clear();
                                        bestIndex.add(k);
                                        mixDist=tmpDist;
                                    }else if (tmpDist==mixDist){
                                        bestIndex.add(k);
                                    }
                                }
                            }
                        }
                    }else {
                        if (mid1>startIndex){
                            for (int k = startIndex; k <mid1+1 ; k++) {
                                ArrayList<Tuple2<Integer,ElectricProfile>> compareDate=dateSet.get(k);
                                if ((!isDel(delDate,compareDate))&&isComplete(compareDate)&&(i!=k)){
                                    finish=true;
                                    double tmpDist=getWeekDist(tmpWeekData,compareDate,calFlag);
                                    if (tmpDist<mixDist){
                                        bestIndex.clear();
                                        bestIndex.add(k);
                                        mixDist=tmpDist;
                                    }else if (tmpDist==mixDist){
                                        bestIndex.add(k);
                                    }
                                }
                            }
                        }
                        if (mid2<endIndex){
                            for (int k = mid2; k <endIndex+1 ; k++) {
                                ArrayList<Tuple2<Integer,ElectricProfile>> compareDate=dateSet.get(k);
                                if ((!isDel(delDate,compareDate))&&isComplete(compareDate)&&(i!=k)){
                                    finish=true;
                                    double tmpDist=getWeekDist(tmpWeekData,compareDate,calFlag);
                                    if (tmpDist<mixDist){
                                        bestIndex.clear();
                                        bestIndex.add(k);
                                        mixDist=tmpDist;
                                    }else if (tmpDist==mixDist){
                                        bestIndex.add(k);
                                    }
                                }
                            }
                        }
                    }

                    if (finish){
                        break;
                    }else {
                        mid1=startIndex-1;
                        mid2=endIndex+1;
                        if (mid1<0){
                            mid1=0;
                        }
                        if (mid2>(dateSet.size()-1)){
                            mid2=dateSet.size()-1;
                        }
                    }
                }
                //endregion

                if (bestIndex.size()>0){
                    int minTimeDist=Integer.MAX_VALUE;
                    int bestSimilarIndex=-1;
                    for (int j = 0; j < bestIndex.size(); j++) {
                        int tmpTimeDist=Math.abs(bestIndex.get(j)-i);
                        if (tmpTimeDist<minTimeDist){
                            minTimeDist=tmpTimeDist;
                            bestSimilarIndex=bestIndex.get(j);
                        }
                    }

                    if (bestSimilarIndex!=-1){
                        ArrayList<Tuple2<Integer,ElectricProfile>> similarest=dateSet.get(bestSimilarIndex);
                        ArrayList<Tuple2<Integer,ElectricProfile>> newTmpWeekDate=new ArrayList<Tuple2<Integer, ElectricProfile>>();

                        for (int j = 0; j < similarest.size(); j++) {
                            if (tmpWeekData.get(j)._1.equals(-1)){
                                String dateStr=tmpWeekData.get(j)._2.getDataDate();
                                //dateStr+=similarest.get(j)._2.getDataDate();
                                //ElectricProfile  tmpEle=similarest.get(j)._2;
                                ElectricProfile  tmpEle=new ElectricProfile(similarest.get(j)._2.toString());
                                tmpEle.setDataDate(dateStr);
                                int mark=similarest.get(j)._1;
                                newTmpWeekDate.add(new Tuple2<Integer, ElectricProfile>(mark,tmpEle));
                                //dateSet.get(i).set(j,new Tuple2<Integer, ElectricProfile>(similarest.get(j)._1,tmpEle));
                                // newTmpWeekDate.add(similarest.get(j));
                            }else {
                                newTmpWeekDate.add(tmpWeekData.get(j));
                            }
                        }

                        retValu.add(newTmpWeekDate);
                    }
                }else {
                    isCompleteForYear=false;
                    break;
                }
            }else {
                retValu.add(tmpWeekData);
            }
        }
        if (!isCompleteForYear){
            ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>> newReturn=new ArrayList<ArrayList<Tuple2<Integer,ElectricProfile>>>();
            for (int i = 0; i < dateSet.size(); i++) {
                ArrayList<Tuple2<Integer,ElectricProfile>> tmpTuple=dateSet.get(i);
                ArrayList<Tuple2<Integer,ElectricProfile>> newTuple=new ArrayList<Tuple2<Integer,ElectricProfile>>();
                for (int j = 0; j < tmpTuple.size(); j++) {
                    newTuple.add(new Tuple2<Integer, ElectricProfile>(-1,tmpTuple.get(j)._2));
                }
                newReturn.add(newTuple);
            }
            return newReturn;
        }
        return retValu;
    }

    public boolean isDel(ArrayList<Integer> delDate,ArrayList<Tuple2<Integer,ElectricProfile>> weekDate){
        boolean flag=false;
        for (int j = 0; j < weekDate.size(); j++) {
            ElectricProfile tmpEle=weekDate.get(j)._2;
            for (int i = 0; i <delDate.size()/2 ; i++) {
                int index=i*2;
                int eleDate=Integer.parseInt(tmpEle.getDataDate());
                if ((eleDate>=delDate.get(index))&&(eleDate<=delDate.get(index+1))){
                    flag=true;
                    break;
                }
            }
            if (flag){
                break;
            }
        }
        return flag;
    }

    public boolean isComplete(ArrayList<Tuple2<Integer,ElectricProfile>> weekDate){
        boolean flag=true;
        for (int i = 0; i < weekDate.size(); i++) {
            if (weekDate.get(i)._1().equals(-1)){
                flag=false;
                break;
            }
        }
        return flag;
    }

    public int getNewDate(String oldDate,int addDaysNum){

        int tmpYear=Integer.parseInt(oldDate.substring(0,4));
        int tmpMonth=Integer.parseInt(oldDate.substring(4,6));
        int tmpDay=Integer.parseInt(oldDate.substring(6,8))+addDaysNum;

        int secondMonthDays=28;

        if ((tmpYear%100)==0){
            if ((tmpYear%400)==0){
                secondMonthDays=29;
            }
        }else {
            if ((tmpYear%4)==0){
                secondMonthDays=29;
            }
        }

        if ((tmpDay>secondMonthDays)&&(tmpMonth==2)){
            tmpMonth=3;
            tmpDay=tmpDay-secondMonthDays;
        }else if ((tmpDay>30)
                &&((tmpMonth==4)
                ||(tmpMonth==6)
                ||(tmpMonth==9)
                ||(tmpMonth==11))){
            tmpMonth=tmpMonth+1;
            tmpDay=tmpDay-30;
        }else if ((tmpDay>31)
                &&((tmpMonth==1)
                ||(tmpMonth==3)
                ||(tmpMonth==5)
                ||(tmpMonth==7)
                ||(tmpMonth==8)
                ||(tmpMonth==10)
                ||(tmpMonth==12)
                )){
            if (tmpMonth==12){
                tmpMonth=1;
                tmpYear=tmpYear+1;
            }else {
                tmpMonth=tmpMonth+1;
            }
            tmpDay=tmpDay-31;
        }

        String dateStr="";
        dateStr+=tmpYear;
        if (tmpMonth<10){
            dateStr+="0"+tmpMonth;
        }else {
            dateStr+=tmpMonth;
        }
        if (tmpDay<10){
            dateStr+="0"+tmpDay;
        }else {
            dateStr+=tmpDay;
        }

        return Integer.parseInt(dateStr);
    }

    public double getWeekDist(ArrayList<Tuple2<Integer,ElectricProfile>> w1,ArrayList<Tuple2<Integer,ElectricProfile>> w2,int distFlag){
        Double retVal=0.0;

        for (int i = 0; i < w1.size(); i++) {
            Tuple2<Integer,ElectricProfile> w1Ele=w1.get(i);
            Tuple2<Integer,ElectricProfile> w2Ele=w2.get(i);

            if (w1Ele._1().equals(-1)){
                retVal+=0;
            }else {
                if (distFlag==0){
                    Distance dist=new Distance();
                    retVal+=Math.pow(dist.getDTWDistance(w1Ele._2.getPoints(),w2Ele._2.getPoints()),2);
                }else if(distFlag==1){
                    Tuple2<Integer,Double> tmp= FftConv.getMaxNNCc(w1Ele._2.getPoints(),w2Ele._2.getPoints());
                    retVal+=Math.pow(1-tmp._2,2);
                }else {
                    Distance dist=new Distance();
                    retVal+=Math.pow(dist.getEuclideanDistance(w1Ele._2.getPoints(),w2Ele._2.getPoints()),2);
                }
            }
        }
        retVal=Math.sqrt(retVal);
        return retVal;
    }

    public static void runDataClear(JavaSparkContext sc,String dataSetStr,
            String savePath, int claDistFlag, String startDay,
            String endDay, String delDate){
        ArrayList<Integer> delDateSet=new ArrayList<Integer>();
        String[] tmpDate=delDate.split(",");
        for (int i = 0; i < tmpDate.length; i++) {
            delDateSet.add(Integer.parseInt(tmpDate[i]));
        }

        class splitOriginDat implements Function<String,ElectricProfile>{
            @Override
            public ElectricProfile call(String s) throws Exception {
                if (s.length()<100){
                    ElectricProfile tmpEle=new ElectricProfile();
                    tmpEle.setID(-1);
                    return tmpEle;
                }else {
                    return new ElectricProfile(s);
                }
            }
        }

        class filterNULLData implements Function<ElectricProfile,Boolean>{
            @Override
            public Boolean call(ElectricProfile electricProfile) throws Exception {
                return electricProfile.getID()!=-1;
            }
        }
        class markRamonID implements PairFunction<ElectricProfile,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(ElectricProfile electricProfile) throws Exception {
                int markID = (int)(Math.random()*35);
                return new Tuple2<Integer, ElectricProfile>(markID,electricProfile);
            }
        }
        class split implements PairFunction<String,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(String s) throws Exception {

                int markID = (int)(Math.random()*35);
                return new Tuple2<Integer, ElectricProfile>(markID,new ElectricProfile(s));
            }
        }

        //SparkConf conf=new SparkConf().setAppName("dataClear");//.setMaster(master).setJars(jarPath);
        //JavaSparkContext sc = new JavaSparkContext(conf);
        DataClear dc=new DataClear();
        JavaRDD<String> lines = sc.textFile(dataSetStr);
        JavaRDD<ElectricProfile> rDataSet=lines.map(new splitOriginDat()).filter(new filterNULLData());
        rDataSet.cache();

        JavaPairRDD<Integer,ElectricProfile> markedDataSet=dc.getPower(rDataSet,0.85).mapToPair(new markRamonID());
        markedDataSet.cache();
        markedDataSet.saveAsTextFile(savePath+"WithoutComplete/");
        rDataSet.unpersist();

        dc.clearDataSet(startDay,endDay,delDateSet,markedDataSet,savePath,claDistFlag);
    }




    public static void main(String[] args) throws IOException {

        String dataSetStr=args[0];
        String savePath=args[1];
        int claDistFlag=Integer.parseInt(args[2]);
        String startDay=args[3];
        String endDay=args[4];
        String delDate=args[5];
        int dataFlag=Integer.parseInt(args[6]);
        int partitionNum=Integer.parseInt(args[7]);
        double thr=Double.parseDouble(args[8]);

        //String kmeansOut=args[6];
        //int clustersNum=Integer.parseInt(args[7]);
        //double convergeDist=Double.parseDouble(args[8]);
        //int maxCount=Integer.parseInt(args[9]);

        ArrayList<Integer> delDateSet=new ArrayList<Integer>();
        String[] tmpDate=delDate.split(",");
        for (int i = 0; i < tmpDate.length; i++) {
            delDateSet.add(Integer.parseInt(tmpDate[i]));
        }

        class splitOriginDat implements Function<String,ElectricProfile>{
            @Override
            public ElectricProfile call(String s) throws Exception {
                if (s.length()<100){
                    ElectricProfile tmpEle=new ElectricProfile();
                    tmpEle.setID(-1);
                    return tmpEle;
                }else {
                    return new ElectricProfile(s);
                }
            }
        }

        class filterNULLData implements Function<ElectricProfile,Boolean>{
            @Override
            public Boolean call(ElectricProfile electricProfile) throws Exception {
                return electricProfile.getID()!=-1;
            }
        }
        class markRamonID implements PairFunction<ElectricProfile,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(ElectricProfile electricProfile) throws Exception {
                int markID = (int)(Math.random()*5000);
                return new Tuple2<Integer, ElectricProfile>(markID,electricProfile);
            }
        }
        class split implements PairFunction<String,Integer,ElectricProfile>{
            @Override
            public Tuple2<Integer, ElectricProfile> call(String s) throws Exception {

                int markID = (int)(Math.random()*5000);
                return new Tuple2<Integer, ElectricProfile>(markID,new ElectricProfile(s));
            }
        }

        SparkConf conf=new SparkConf().setAppName("dataClear");//.setMaster(master).setJars(jarPath);
        JavaSparkContext sc = new JavaSparkContext(conf);
        DataClear dc=new DataClear();
        if (dataFlag==0){
            JavaRDD<String> lines = sc.textFile(dataSetStr).coalesce(partitionNum);
            JavaRDD<ElectricProfile> rDataSet=lines.map(new splitOriginDat()).filter(new filterNULLData());
            //rDataSet.cache();

            JavaPairRDD<Integer,ElectricProfile> markedDataSet=dc.getPower(rDataSet,thr).mapToPair(new markRamonID());
            //markedDataSet.cache();
            markedDataSet.saveAsTextFile(savePath+"WithoutComplete/");
            //rDataSet.unpersist();
            //lines.mapToPair(new SplitPairELEWithMarkID()).coalesce(1).saveAsTextFile(savePath);
            JavaRDD<String> lines2=sc.textFile(savePath+"WithoutComplete/");
            JavaPairRDD<Integer,ElectricProfile> markedDataSet2=lines2.mapToPair(new SplitPairELEWithMarkID());
            dc.clearDataSet(startDay,endDay,delDateSet,markedDataSet2,savePath,claDistFlag);
            //markedDataSet.unpersist();

            //KMeans.KMeansRun(savePath+"complete/",kmeansOut,clustersNum,convergeDist,maxCount,claDistFlag,sc);
        }else {
            JavaRDD<String> lines2=sc.textFile(dataSetStr).coalesce(partitionNum);
            JavaRDD<ElectricProfile> rDataSet2=lines2.map(new splitOriginDat()).filter(new filterNULLData());
            JavaPairRDD<Integer,ElectricProfile> markedDataSet2=rDataSet2.mapToPair(new markRamonID());
            dc.clearDataSet(startDay,endDay,delDateSet,markedDataSet2,savePath,claDistFlag);
        }



    }
}
