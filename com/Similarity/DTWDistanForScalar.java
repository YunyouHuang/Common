package com.Similarity;

import com.Data.FloatMatrix;
import org.ujmp.core.Matrix;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 数值为离散值的序列的动态扭曲算法
 * Created by hdp on 17-1-4.
 */
public class DTWDistanForScalar implements Serializable{

    public double getDTWDistanceForScalar(ArrayList<Integer> a, ArrayList<Integer> b, Matrix distMatrix){
        Matrix euDistance=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                euDistance.setAsDouble(Math.abs(distMatrix.getAsDouble(a.get(i),b.get(j))),i,j);
            }
        }

        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    dpMatrix.setAsDouble(euDistance.getAsDouble(i,j),i,j);
                }else{
                    double iMinus2jMinus=Double.MAX_VALUE;
                    double iMinus2j=Double.MAX_VALUE;
                    double i2jMinus=Double.MAX_VALUE;
                    int indexiMinus=i-1;
                    int indexjMinus=j-1;

                    if ((indexiMinus>=0)&&(indexjMinus>=0)){
                        iMinus2jMinus=dpMatrix.getAsDouble(indexiMinus,indexjMinus);
                        iMinus2j=dpMatrix.getAsDouble(indexiMinus,j);
                        i2jMinus=dpMatrix.getAsDouble(i,indexjMinus);
                    }else if ((indexiMinus<0)&&(indexjMinus>=0)){
                        i2jMinus=dpMatrix.getAsDouble(i,indexjMinus);
                    }else if ((indexiMinus>=0)&&(indexjMinus<0)){
                        iMinus2j=dpMatrix.getAsDouble(indexiMinus,j);
                    }
                    dpMatrix.setAsDouble(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+euDistance.getAsDouble(i,j),i,j);
                }
            }
        }

        return dpMatrix.getAsDouble(a.size()-1,b.size()-1);
    }
    public double getDTWDistanceForScalar(ArrayList<Integer> a, ArrayList<Integer> b, FloatMatrix distMatrix,int dcSize){
        Matrix euDistance=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                euDistance.setAsDouble(Math.abs(distMatrix.getValue(a.get(i),b.get(j),dcSize)),i,j);
            }
        }

        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    dpMatrix.setAsDouble(euDistance.getAsDouble(i,j),i,j);
                }else{
                    double iMinus2jMinus=Double.MAX_VALUE;
                    double iMinus2j=Double.MAX_VALUE;
                    double i2jMinus=Double.MAX_VALUE;
                    int indexiMinus=i-1;
                    int indexjMinus=j-1;

                    if ((indexiMinus>=0)&&(indexjMinus>=0)){
                        iMinus2jMinus=dpMatrix.getAsDouble(indexiMinus,indexjMinus);
                        iMinus2j=dpMatrix.getAsDouble(indexiMinus,j);
                        i2jMinus=dpMatrix.getAsDouble(i,indexjMinus);
                    }else if ((indexiMinus<0)&&(indexjMinus>=0)){
                        i2jMinus=dpMatrix.getAsDouble(i,indexjMinus);
                    }else if ((indexiMinus>=0)&&(indexjMinus<0)){
                        iMinus2j=dpMatrix.getAsDouble(indexiMinus,j);
                    }
                    dpMatrix.setAsDouble(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+euDistance.getAsDouble(i,j),i,j);
                }
            }
        }

        return dpMatrix.getAsDouble(a.size()-1,b.size()-1);
    }
}
