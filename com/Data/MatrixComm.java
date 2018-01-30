package com.Data;

import org.ujmp.core.Matrix;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by hdp on 16-9-12.
 */
public class MatrixComm implements Serializable {

    public Matrix Array2Matrix(ArrayList<Double> data){
        Matrix retValue=Matrix.Factory.zeros(data.size(),1);
        for (int i = 0; i < data.size(); i++) {
            retValue.setAsDouble(data.get(i),i,0);
        }
        return retValue;
    }

    public String matrix2Str(Matrix x){
        String retValue="";
        for (int i = 0; i < x.getRowCount(); i++) {
            for (int j = 0; j < x.getColumnCount(); j++) {
                if ((i==0)&&(j==0)){
                    retValue+=x.getAsDouble(i,j);
                }else{
                    retValue+=","+x.getAsDouble(i,j);
                }
            }
        }
        return retValue;
    }

    public Matrix str2Matrix(String str,int row,int col){
        String[] data=str.split(",");
        Matrix retValue=Matrix.Factory.zeros(row,col);
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < col; j++) {
                retValue.setAsString(data[i*row+j],i,j);
            }
        }
        return retValue;
    }

    public ArrayList<Matrix> str2MatrixArray(String str,int row,int col){
        ArrayList<Matrix> retValue=new ArrayList<Matrix>();
        String[] data=str.split("-");
        for (int i = 0; i < data.length; i++) {
            retValue.add(str2Matrix(data[i],row,col));
        }
        return retValue;
    }

    //列主矩阵的数组转换成矩阵
    public Matrix doubleArray2Matrix(double[] data,int row,int col){
        Matrix retValue=Matrix.Factory.zeros(row,col);
        for (int i = 0; i < col; i++) {
            for (int j = 0; j < row; j++) {
                retValue.setAsDouble(data[i*col+j],j,i);
            }
        }
        return retValue;
    }

    public Jama.Matrix doubleArray2MatrixJAMA(double[] data,int row,int col){
        //Matrix retValue=Matrix.Factory.zeros(row,col);
        double[][] _matrix=new double[row][col];

        for (int i = 0; i < col; i++) {
            for (int j = 0; j < row; j++) {
                _matrix[i][j]=data[i*col+j];
            }
        }

        return new Jama.Matrix(_matrix);

    }


    public Jama.Matrix eyeJAMA(int row){
        double[][] _matrix=new double[row][row];
        for (int i = 0; i < row; i++) {
            for (int j = 0; j < row; j++) {
                if (i==j){
                    _matrix[i][j]=1;
                }else {
                    _matrix[i][j]=0;
                }

            }
        }
        return new Jama.Matrix(_matrix);

    }

    public ArrayList<Double> getMaxEigenvector(Jama.Matrix dMatrix,Jama.Matrix vMatrix){
        ArrayList<Double> retValue=new ArrayList<Double>();
        int bestIndex=-1;
        double _max=Double.MIN_VALUE;
        for (int i = 0; i < dMatrix.getRowDimension(); i++) {
            if (dMatrix.get(i,i)>_max){
                bestIndex=i;
                _max=dMatrix.get(i,i);
            }
        }
        for (int i = 0; i < vMatrix.getColumnDimension(); i++) {
            retValue.add(vMatrix.get(i,bestIndex));
        }
        return retValue;

    }
}
