package com.Data;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 周片段或年片段的实例
 * Created by hdp on 17-1-4.
 */
public class WeekAndYearInstance implements Serializable{
    long ID=-1;
    String date="";
    ArrayList<Integer> data=new ArrayList<Integer>();


    public long getID() {
        return ID;
    }

    public void setID(long iD) {
        ID = iD;
    }

    public String getDate(){
        return this.date;
    }

    public void setDate(String s){
        this.date=s;
    }

    public int getDataSize() {
        return data.size();
    }

    public ArrayList<Integer> getData() {
        ArrayList<Integer> tmpData=new ArrayList<Integer>();
        tmpData.addAll(data);
        return tmpData;
    }

    public void setData(ArrayList<Integer> data) {
        this.data.addAll(data);
    }

    public WeekAndYearInstance(){

    }

    public WeekAndYearInstance(String str){
        String[] tmpStr=str.split(",");
        this.ID=Long.parseLong(tmpStr[0]);
        this.date=tmpStr[1];
        for (int i = 2; i < tmpStr.length; i++) {
            this.data.add(Integer.parseInt(tmpStr[i]));
        }
    }

    public String toString() {
        String retValue="";
        retValue=ID+","+date;
        for (int i = 0; i < data.size(); i++) {
            retValue+=","+data.get(i);
        }
        return retValue;
    }
}
