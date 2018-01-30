package com.Data;

import java.util.ArrayList;

/**
 * Created by hyy on 2017/11/25.
 */
public class SplitEMG {

    public ArrayList<ArrayList<Double>> splitEmg(ArrayList<ArrayList<Double>> data,int train,double waveRate,int pre){
        ArrayList<ArrayList<Double>> retValue=new ArrayList<ArrayList<Double>>();

        return retValue;
    }

    public ArrayList<Integer> getStartAndEnd(ArrayList<Double> data,int train,double waveRate,int pre){
        ArrayList<Integer> retValue=new ArrayList<Integer>();
        return retValue;
    }

    public int getSatrt(ArrayList<Double> data,int train,double waveRate,int pre){

        return 0;
    }

    public int getEnd(ArrayList<Double> data,int train,double waveRate,int pre){

        return 0;
    }

    public ArrayList<TIntAndDouble> getWaveForStart(ArrayList<Double>data){
        ArrayList<TIntAndDouble> retValue=new ArrayList<TIntAndDouble>();
        return retValue;
    }
    public ArrayList<TIntAndDouble> getWave(ArrayList<Double> data){
        ArrayList<Integer> fArray=firstNorDifference(data);
        ArrayList<Integer> sArray=secondNorDifference(fArray);

        ArrayList<TIntAndDouble> retValue=new ArrayList<TIntAndDouble>();

        for (int i = 0; i < fArray.size(); i++) {
            if ((fArray.get(i)==-1)||(sArray.get(i)==-2)){
                TIntAndDouble temWave=new TIntAndDouble(i,data.get(i));
                retValue.add(temWave);
            }
        }
        return retValue;
    }

    public ArrayList<Integer> firstNorDifference(ArrayList<Double> data){
        ArrayList<Integer> retValue=new ArrayList<Integer>();
        if (data.size()>1){
            for (int i = 1; i < data.size(); i++) {
                double pre=data.get(i-1);
                double cur=data.get(i);
                if (cur>pre){
                    retValue.add(1);
                }else if (cur==pre){
                    retValue.add(0);
                }else {
                    retValue.add(-1);
                }
            }
            retValue.add(0);
        }else {
            System.out.println("The data's length must bigger than 1!");
        }
        return retValue;
    }

    public  ArrayList<Integer> secondNorDifference(ArrayList<Integer> data){
        ArrayList<Integer> retValue=new ArrayList<Integer>();
        if (data.size()>1){
            retValue.add(0);
            for (int i = 1; i < data.size(); i++) {
                int pre=data.get(i-1);
                int cur=data.get(i);
                retValue.add(cur-pre);
            }
        }else {
            System.out.println("The data's length must bigger than 1!");
        }
        return retValue;
    }

    public static void main(String[] args)  {

    }

    public class TIntAndDouble{
        public int _1;
        public double _2;

        public TIntAndDouble(int x,double y){
            _1=x;
            _2=y;
        }

        public int get_1(){
            return _1;
        }
        public void set_1(int x){
            _1=x;
        }

        public double get_2(){
            return _2;
        }
        public void set_2(double y){
            _2=y;
        }
    }
}
