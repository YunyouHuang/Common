package com.Data;

import java.io.Serializable;

/**
 * Created by hdp on 17-5-12.
 */
public class FloatMatrix implements Serializable{


    //ArrayList<ArrayList<Float>> floatMatrix=new ArrayList<ArrayList<Float>>();
    float[][] floaltMatrix;

    public FloatMatrix(long x){
        //this.matrix=new float[(x*x-x)/2];
        int row=getRow(x);
        int col=getCol(x);
        this.floaltMatrix=new float[col][row];
        for (int i = 0; i < col; i++) {
            for (int j = 0; j < row; j++) {
                floaltMatrix[i][j]=0;
            }
        }
    }
    public Boolean setValue(float value,long x,long y,long length){
        if (x>=y) {
            //System.out.println("不合符对称矩阵要求的位置！");
            return false;
        }else {
            double _length=(double)length;
            long totalLength=(long)((((_length-1)+(_length-1-x))/2)*(x+1)-(_length-1-y));
            double row=getRow(length);
            //int col=getRow(length);
            int rowIndex;
            if (totalLength%row==0) {
                rowIndex=(int)(totalLength/row)-1;
            }else {
                rowIndex=(int)(totalLength/row);
            }
            int colIndex=(int)(totalLength-row*rowIndex)-1;
            this.floaltMatrix[rowIndex][colIndex]=value;
            return true;
        }
    }

    public Float getValue(long x,long y,long length) {
        if (x>y) {
            //System.out.println("不合符对称矩阵要求的位置！");
            //return Float.MAX_VALUE;
            return _getValue(y,x,length);
        }else if(x==y){
            return 0.0f;
        }else {
            return _getValue(x,y,length);
        }
    }

    public float _getValue(long x,long y,long length){
        double _length=(double)length;
        long totalLength=(long)((((_length-1)+(_length-1-x))/2)*(x+1)-(_length-1-y));
        int row=getRow(length);
        //int col=getRow(length);
        int rowIndex;
        if (totalLength%row==0) {
            rowIndex=(int)(totalLength/row)-1;
        }else {
            rowIndex=(int)(totalLength/row);
        }
        int colIndex=(int)(totalLength-row*rowIndex)-1;
        return this.floaltMatrix[rowIndex][colIndex];
    }
    public String matrixTostring(long length) {
        String retVuleString="";
        int row=getRow(length);
        int col=getCol(length);

        for (int i = 0; i < length; i++) {
            for (int j = 0; j < length; j++) {
                if (i<j) {
                    retVuleString+="    "+String .format("%.4f",getValue(i, j, length));
                    if (j==(length-1)) {
                        retVuleString+="\n";
                    }
                }else{
                    if (j==0) {
                        retVuleString+="NULL";
                    }else if (j==(length-1)) {
                        retVuleString+="    NULL  \n";
                    }else {
                        retVuleString+="    NULL  ";
                    }
                }
            }
        }
        return retVuleString;
    }
    public int getRow(long x) {
        long length=(x*x-x)/2;
        return (int)Math.sqrt(length);
    }

    public int  getCol(long x) {
        long length=(x*x-x)/2;
        return (int)(length/getRow(x))+1;
    }
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        System.out.println((int)0.6);
        FloatMatrix fMatrix=new FloatMatrix(8000);
        //fMatrix.setValue(0.333f, 4, 4, 5);
        System.out.println(fMatrix.matrixTostring(50));
    }


}
