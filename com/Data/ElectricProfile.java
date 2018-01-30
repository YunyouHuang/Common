package com.Data;

import java.io.Serializable;
import java.util.ArrayList;
/**
 * 表示数据点
 * Created by hdp on 16-9-7.
 */
public class ElectricProfile implements Serializable{

    //数据库表字段
    long ID;
    String dataDate;
    int dataType;
    String orgNO;
    String getDate;
    int dataPointFlag;
    String dataWholeFlag;
    ArrayList<Double> points=new ArrayList<Double>();

    long complexNum=1;//指示复合曲线，表示表征的曲线的数量。主要用于中间过程计算，以及密度聚类的局部解的表示。


    public long getID() {
        return ID;
    }
    public void setID(long iD) {
        ID = iD;
    }
    public String getDataDate() {
        return dataDate;
    }
    public void setDataDate(String dataDate) {
        this.dataDate = dataDate;
    }
    public int getDataType() {
        return dataType;
    }
    public void setDataType(int dataType) {
        this.dataType = dataType;
    }
    public String getOrgNO() {
        return orgNO;
    }
    public void setOrgNO(String orgNO) {
        this.orgNO = orgNO;
    }
    public String getGetDate() {
        return getDate;
    }
    public void setGetDate(String getDate) {
        this.getDate = getDate;
    }
    public int getDataPointFlag() {
        return dataPointFlag;
    }
    public void setDataPointFlag(int dataPointFlag) {
        this.dataPointFlag = dataPointFlag;
    }
    public String getDataWholeFlag() {
        return dataWholeFlag;
    }
    public void setDataWholeFlag(String dataWholeFlag) {
        this.dataWholeFlag = dataWholeFlag;
    }
    public ArrayList<Double> getPoints() {
        return points;
    }
    public void setPoints(ArrayList<Double> newPoints) {
        this.points =new ArrayList<Double>();
        for (int i = 0; i < newPoints.size(); i++) {
            points.add(newPoints.get(i));
        }
    }
    public void setComplexNum(long num){
        this.complexNum=num;
    }
    public long getComplexNum(){
        return this.complexNum;
    }


    public ElectricProfile(){

    }

    public  ElectricProfile(String profile) {
        String[] profiles;
        if (profile.contains(",")){
            String lastChara=profile.substring(profile.length()-1,profile.length());
            if (lastChara.equals(",")){
                profile+="0.0";
            }
            profiles=profile.split(",");
        }else if (profile.contains(";")){
            if (profile.substring(profile.length()-1,profile.length()).equals(";")){
                profile+="0.0";
            }
            profiles=profile.split(";");
        }else if (profile.contains(" ")){
            //if (profile.substring(profile.length()-1,profile.length()).equals(" ")){
               // profile+="0.0";
            //}
            profiles=profile.split(" ");
        } else{
            if (profile.substring(profile.length()-1,profile.length()).equals("\t")){
                profile+="0.0";
            }
            profiles=profile.split("\t");
        }

        this.ID=Long.parseLong(profiles[0]);
        this.dataDate=profiles[1];
        this.dataType=Integer.parseInt(profiles[2]);
        this.orgNO=profiles[3];
        this.getDate=profiles[4];
        this.dataPointFlag=Integer.parseInt(profiles[5]);
        this.dataWholeFlag=profiles[6];
        for (int i= 7; i <profiles.length; i++) {
            String tmpValue=profiles[i];
            if (tmpValue.trim().equals("")){
                if (i==(profiles.length-1)){
                    profiles[i]="0.0";
                }else{
                    int nextData=i;
                    for (int j = i+1; j <profiles.length ; j++) {
                        if (!profiles[j].trim().equals("")){
                            nextData=j;
                            break;
                        }
                    }
                    if (nextData==i){
                        for (int j = i; j <profiles.length ; j++) {
                            profiles[j]="0.0";
                        }
                    }else{
                        double preData=0;
                        if (i!=7){
                            preData=Double.parseDouble(profiles[i-1]);
                        }
                        double nextDataValue=Double.parseDouble(profiles[nextData]);

                        double interValue=(nextDataValue-preData)/(nextData-i+1);
                        for (int j = i; j <nextData ; j++) {
                            profiles[j]=String.valueOf(preData+(j-i+1)*interValue);
                        }
                    }
                }
            }
            points.add(Double.parseDouble(profiles[i]));
        }
    }
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        String data="356654,2016-09-22,1,kjhhjkh,2016-09-22,96,1111111111111111111111111111111,,17.250932082100782,1.7578180733168436,,7.466882465588491,4.0073097722396085,3.7134214624098694,7.746066282070149,,,,,4.115089966742307,8.278847377929761,0.3321730516069654,,,";

        ElectricProfile el=new ElectricProfile(data);
        System.out.println(el.toString());
    }


    public String toString() {
        String retValue="";
        retValue=ID+","+dataDate+","+dataType+","+orgNO+","+getDate+","+dataPointFlag+","+dataWholeFlag;
        for (int i = 0; i < points.size(); i++) {
            retValue+=","+points.get(i);
        }
        return retValue;
    }
}
