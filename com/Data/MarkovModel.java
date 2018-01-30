package com.Data;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.ujmp.core.Matrix;
import org.ujmp.core.calculation.Calculation.Ret;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * 马尔科夫模型，每一个用户以一系列马尔科夫概率转移矩阵表示
 * Created by hdp on 16-9-11.
 */
public class MarkovModel {

    public JavaPairRDD<Long, ArrayList<Matrix>> getMarkovModel(JavaRDD<ElectricProfile>dataSet, int stateNum, int demNum){

        final int stateNumber=stateNum;
        final int dataDeminNum=demNum;
        class markCustomerID implements PairFunction<ElectricProfile,Long,ElectricProfile>{

            @Override
            public Tuple2<Long, ElectricProfile> call(ElectricProfile electricProfile) throws Exception {
                return new Tuple2<Long, ElectricProfile>(new Long(electricProfile.getID()),electricProfile);
            }
        }

        class getMarkovModel implements PairFunction<Tuple2<Long,Iterable<ElectricProfile>>,Long,ArrayList<Matrix>>{

            @Override
            public Tuple2<Long, ArrayList<Matrix>> call(Tuple2<Long, Iterable<ElectricProfile>> longIterableTuple2) throws Exception {
                ArrayList<Matrix> customerMarkovModelNum=new ArrayList<Matrix>();
                for (int i = 0; i < dataDeminNum-1; i++) {
                    Matrix transNum=Matrix.Factory.zeros(stateNumber,stateNumber);
                    customerMarkovModelNum.add(transNum);
                }
                for (ElectricProfile el : longIterableTuple2._2) {
                    ArrayList<Double> points=el.getPoints();
                    for (int i = 0; i < points.size()-1; i++) {
                        int r1= (int) (double)points.get(i);
                        int r2= (int) (double)points.get(i+1);
                        Long index1=new Long(r1);
                        Long index2=new Long(r2);
                        Matrix transNum=customerMarkovModelNum.get(i);
                        transNum.setAsLong(transNum.getAsLong(index1,index2)+1,index1,index2);
                    }
                }

                ArrayList<Matrix> customerMarkovModelPro=new ArrayList<Matrix>();
                for (int i = 0; i < customerMarkovModelNum.size(); i++) {
                    Matrix transNum=customerMarkovModelNum.get(i);
                    Matrix transPro=Matrix.Factory.zeros(stateNumber,stateNumber);
                    for (int j = 0; j <stateNumber; j++) {
                        double rowSum=transNum.selectRows(Ret.NEW,new Long(j)).getValueSum();
                        if (rowSum!=0.0){
                            for (int k = 0; k < stateNumber; k++) {
                                transPro.setAsDouble(transNum.getAsDouble(j,k)/rowSum,j,k);
                            }
                        }
                    }
                    customerMarkovModelPro.add(transPro);
                }

                return new Tuple2<Long, ArrayList<Matrix>>(longIterableTuple2._1,customerMarkovModelPro);
            }
        }
        return dataSet.mapToPair(new markCustomerID()).groupByKey().mapToPair(new getMarkovModel());

    }

    public static void main(String[] args){
        //getMarkovModel();
    }
}
