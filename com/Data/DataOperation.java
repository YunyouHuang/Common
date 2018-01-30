package com.Data;

import JavaTuple.Tupleid;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 用于数组的加减乘除等一系列的数据操作
 * Created by hdp on 16-9-10.
 */
public class DataOperation implements Serializable {

    public ArrayList<Double> ArrayMult(ArrayList<Double> data, double k){
        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < data.size(); i++) {
            retValue.add(data.get(i)*k);
        }
        return retValue;
    }

    public static ArrayList<Float> getFloatArray(String s){
        String[] tmpStr=s.split(",");
        ArrayList<Float> retValue=new ArrayList<Float>();
        for (int i = 7; i < tmpStr.length; i++) {
            retValue.add(Float.parseFloat(tmpStr[i]));
        }
        return retValue;
    }

    public ArrayList<Double> ArrayDivid(ArrayList<Double> data,double k){
        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < data.size(); i++) {
            retValue.add(data.get(i)/k);
        }
        return retValue;
    }

    public static ArrayList<Float> Doubel2Float(ArrayList<Double> data){
        ArrayList<Float> retValue=new ArrayList<Float>();
        for (int i = 0; i < data.size(); i++) {
            retValue.add(Float.parseFloat(String.valueOf(data.get(i))));
        }
        return retValue;
    }

    public static ArrayList<Double> Float2Doubel(ArrayList<Float> data){
        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < data.size(); i++) {
            retValue.add(Double.parseDouble(String.valueOf(data.get(i))));
        }
        return retValue;
    }

    public static float getSumForflatArray(ArrayList<Float> data){
        float retValue=0;
        for (int i = 0; i < data.size(); i++) {
            retValue+=data.get(i);
        }
        return retValue;
    }

    public double getArrayModel(ArrayList<Double> data){
        double retValue=0;
        if (data.size()<1){
            System.out.println("数组为空数组！");
            return -Double.MAX_VALUE;
        }else {
            for (int i = 0; i < data.size(); i++) {
                retValue+=Math.pow(data.get(i),2);
            }
            retValue=Math.sqrt(retValue);
            return retValue;
        }
    }

    public List<Tupleid> sortArray(ArrayList<Double> data){
        List<Tupleid> retValue=new ArrayList<Tupleid>();
        for (int i = 0; i < data.size(); i++) {
            retValue.add(new Tupleid(i,data.get(i)));
        }
        Collections.sort(retValue);
        return retValue;
    }

    public String listTupleidToStr(List<Tupleid> data){

        String retValue="";
        for (int i = data.size()-1; i >-1; i--) {
            Tupleid tmpData=data.get(i);
            if (i==(data.size()-1)){
                retValue+=tmpData._2+","+tmpData._1;
            }else {
                retValue+=","+tmpData._2+","+tmpData._1;
            }
        }
        return  retValue;
    }

    public double getArrayMax(ArrayList<Double> data){
        double retValue=-Double.MAX_VALUE;
        if (data.size()<1){
            System.out.println("数组为空数组！");
            return -Double.MAX_VALUE;
        }else {
            for (int i = 0; i < data.size(); i++) {
                if (data.get(i)>retValue){
                    retValue=data.get(i);
                }
            }
            return retValue;
        }
    }

    public List<Double> sortArrayForDouble(ArrayList<Double> data){
        List<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < data.size(); i++) {
            retValue.add(data.get(i));
        }
        Collections.sort(retValue);
        return retValue;
    }

    public ArrayList<Double> arrayAppend(ArrayList<Double> a,ArrayList<Double> b){
        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < a.size(); i++) {
            retValue.add(a.get(i));
        }
        for (int i = 0; i < b.size(); i++) {
            retValue.add(b.get(i));
        }
        return retValue;
    }

    public ArrayList<Double> arrayAppendAlong(ArrayList<Double> a,ArrayList<Double> b){
        for (int i = 0; i < b.size(); i++) {
            a.add(b.get(i));
        }
        return a;
    }

    public String getArrayMaxWithIndex(ArrayList<Double> data){
        double retValue=-Double.MAX_VALUE;
        if (data.size()<1){
            System.out.println("数组为空数组！");
            return "";
        }else {
            int bestIndex=-1;
            for (int i = 0; i < data.size(); i++) {
                if (data.get(i)>retValue){
                    retValue=data.get(i);
                    bestIndex=i;
                }
            }
            return retValue+","+bestIndex;
        }
    }

    public String getArrayMinWithIndex(ArrayList<Double> data){
        double retValue=Double.MAX_VALUE;
        if (data.size()<1){
            System.out.println("数组为空数组！");
            return "";
        }else {
            int bestIndex=-1;
            for (int i = 0; i < data.size(); i++) {
                if (data.get(i)<retValue){
                    retValue=data.get(i);
                    bestIndex=i;
                }
            }
            return retValue+","+bestIndex;
        }
    }

    public double getArrayMin(ArrayList<Double> data){
        double retValue=Double.MAX_VALUE;
        if (data.size()<1){
            System.out.println("数组为空数组！");
            return Double.MAX_VALUE;
        }else {
            for (int i = 0; i < data.size(); i++) {
                if (data.get(i)<retValue){
                    retValue=data.get(i);
                }
            }
            return retValue;
        }
    }

    public double getArrayMean(ArrayList<Double> data){
        double retValue=0;
        if (data.size()<1){
            System.out.println("数组为空数组！");
            return Double.MAX_VALUE;
        }else {
            for (int i = 0; i < data.size(); i++) {
                    retValue+=data.get(i);
            }
            return retValue/data.size();
        }
    }

    public double collectArray(ArrayList<Double> data){
        double retValue=0;
        if (data.size()<1){
            System.out.println("数组为空数组！");
            return -Double.MAX_VALUE;
        }else {
            for (int i = 0; i < data.size(); i++) {
                retValue=data.get(i);
            }
            return retValue;
        }
    }

    public ArrayList<Double> arrayPlu(ArrayList<Double> a,ArrayList<Double> b){
        ArrayList<Double> retValue=new ArrayList<Double>();
        if (a.size()==0){
            retValue.addAll(b);
        }
        else if (b.size()==0){
            retValue.addAll(a);
        }
        else if(a.size()==b.size()){
            for (int i = 0; i < a.size(); i++) {
                retValue.add(a.get(i)+b.get(i));
            }
        }else {
            System.out.println("数组长度不一样！");
        }
        return retValue;
    }

    public static ArrayList<Float> arrayPluForFloat(ArrayList<Float> a,ArrayList<Float> b){
        ArrayList<Float> retValue=new ArrayList<Float>();
        if (a.size()==0){
            retValue.addAll(b);
        }
        else if (b.size()==0){
            retValue.addAll(a);
        }
        else if(a.size()==b.size()){
            for (int i = 0; i < a.size(); i++) {
                retValue.add(a.get(i)+b.get(i));
            }
        }else {
            System.out.println("数组长度不一样！");
        }
        return retValue;
    }

    public static ArrayList<Integer> arrayPluForInt(ArrayList<Integer> a,ArrayList<Integer> b){
        ArrayList<Integer> retValue=new ArrayList<Integer>();
        if (a.size()==0){
            retValue.addAll(b);
        }
        else if (b.size()==0){
            retValue.addAll(a);
        }
        else if(a.size()==b.size()){
            for (int i = 0; i < a.size(); i++) {
                retValue.add(a.get(i)+b.get(i));
            }
        }else {
            System.out.println("数组长度不一样！");
        }
        return retValue;
    }

    public ArrayList<Double> arrayMinus(ArrayList<Double> a,ArrayList<Double> b){
        ArrayList<Double> retValue=new ArrayList<Double>();
        if (a.size()==0){
            retValue.addAll(b);
        }
        else if (b.size()==0){
            retValue.addAll(a);
        }
        else if(a.size()==b.size()){
            for (int i = 0; i < a.size(); i++) {
                retValue.add(a.get(i)-b.get(i));
            }
        }else {
            System.out.println("数组长度不一样！");
        }
        return retValue;
    }

    public static String ArrayToStr(ArrayList<Double> data){
        String retValue="";
        for (int i = 0; i < data.size(); i++) {
            if (i==0){
                retValue+=data.get(i);
            }else{
                retValue+=","+data.get(i);
            }
        }
        return retValue;
    }

    public static String ArrayToStrForFloat(ArrayList<Float> data){
        String retValue="";
        for (int i = 0; i < data.size(); i++) {
            if (i==0){
                retValue+=data.get(i);
            }else{
                retValue+=","+data.get(i);
            }
        }
        return retValue;
    }

    public static String AArrayToStr(ArrayList<ArrayList<Double>> data){
        String retValue="";
        for (int i = 0; i < data.size(); i++) {
            if (i==0){
                retValue+=ArrayToStr(data.get(i));
            }else{
                retValue+="\n"+ArrayToStr(data.get(i));
            }
        }
        return retValue;
    }

    public ArrayList<Double> arrayPow(ArrayList<Double> a){
        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < a.size(); i++) {
            retValue.add(Math.pow(a.get(i),2));
        }
        return retValue;
    }

    public static ArrayList<Integer> strToArrayForInt(String string){
        String[] tmpStr=string.split(",");
        ArrayList<Integer> retValue=new ArrayList<Integer>();
        for (int i = 0; i < tmpStr.length; i++) {
            retValue.add(Integer.parseInt(tmpStr[i]));
        }
        return retValue;
    }

    public static ArrayList<Double> strToArray(String profile){
        ArrayList<Double> retValue=new ArrayList<Double>();
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
        }else{
            if (profile.substring(profile.length()-1,profile.length()).equals("\t")){
                profile+="0.0";
            }
            profiles=profile.split("\t");
        }
        for (int i= 0; i <profiles.length; i++) {
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
                        if (i!=0){
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
            retValue.add(Double.parseDouble(profiles[i]));
        }

        return retValue;
    }

    public static double getSum(ArrayList<Double> points){
        double sum=0;
        for (int i = 0; i <points.size(); i++) {
            sum+=points.get(i);
        }
        return sum;
    }

    public static float getSumForFloat(ArrayList<Float> points){
        float sum=0;
        for (int i = 0; i <points.size(); i++) {
            sum+=points.get(i);
        }
        return sum;
    }

    public static ArrayList<Float> arrayDouble2Float(ArrayList<Double> data){
        ArrayList<Float> retValue=new ArrayList<Float>();
        for (int i = 0; i < data.size(); i++) {
            double tmp=data.get(i);
            retValue.add((float)tmp);
        }
        return retValue;
    }
}
