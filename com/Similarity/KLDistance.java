package com.Similarity;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.ujmp.core.Matrix;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 计算两个转移矩阵数组的KL散度
 * Created by hdp on 16-9-7.
 */
public class KLDistance implements Serializable {
    public double getDistance(JSONArray matrixMulp1, JSONArray matrixMulp2, int stateNum) {
        double retValue=0;
        if (matrixMulp1.size()==matrixMulp2.size()) {
            double tmpValue=0;
            for (int i = 0; i < matrixMulp1.size(); i++) {
                JSONObject tmpMatrix1=matrixMulp1.getJSONObject(i);
                JSONObject tmpMatrix2=matrixMulp2.getJSONObject(i);

                double tmpKLD1=0;
                double tmpKLD2=0;
                for (int j = 0; j < stateNum; j++) {
                    for (int j2 = 0; j2 <stateNum; j2++) {
                        String d1=String.valueOf(j);
                        String d2=String.valueOf(j2);
                        double matrix1Ele=Math.pow(2, -40);
                        double matrix2Ele=Math.pow(2, -40);
                        if (tmpMatrix1.containsKey(d1)) {
                            if (tmpMatrix1.getJSONObject(d1).containsKey(d2)) {
                                matrix1Ele=tmpMatrix1.getJSONObject(d1).getDouble(d2);
                            }
                        }
                        if (tmpMatrix2.containsKey(d1)) {
                            if (tmpMatrix2.getJSONObject(d1).containsKey(d2)) {
                                matrix2Ele=tmpMatrix2.getJSONObject(d1).getDouble(d2);
                            }
                        }

                        double transMatrix1Ele;
                        double transMatrix2Ele;
                        if (matrix1Ele==Math.pow(2, -40)) {
                            transMatrix1Ele=0;
                        }else {
                            transMatrix1Ele=matrix1Ele;
                        }

                        if (matrix2Ele==0) {
                            transMatrix2Ele=Math.pow(2, -40);
                        } else {
                            transMatrix2Ele=matrix2Ele;
                        }

                        tmpKLD1+=transMatrix1Ele*Math.log(transMatrix1Ele/transMatrix2Ele);


                        double revTransMatrix1Ele;
                        double revTransMatrix2Ele;
                        if (matrix2Ele==Math.pow(2, -40)) {
                            revTransMatrix1Ele=0;
                        }else {
                            revTransMatrix1Ele=matrix2Ele;
                        }

                        if (matrix1Ele==0) {
                            revTransMatrix2Ele=Math.pow(2, -40);
                        } else {
                            revTransMatrix2Ele=matrix1Ele;
                        }

                        tmpKLD2+=revTransMatrix1Ele*Math.log(revTransMatrix1Ele/revTransMatrix2Ele);
                    }
                }

                tmpValue=(tmpKLD1+tmpKLD2)/(2*stateNum);
            }

            retValue+=tmpValue;
        } else {
            retValue=Double.MAX_VALUE;
        }
        return retValue;
    }


    public double getKLDistance(ArrayList<Matrix> a,ArrayList<Matrix> b){
        double retValue=0;
        if (a.size()!=b.size()){
            System.out.println("size not compatible!");
            return Double.MAX_VALUE;
        }else {
            for (int i = 0; i < a.size(); i++) {
                Matrix aSegMatrix=a.get(i);
                Matrix bSegMatrix=b.get(i);
                if ((aSegMatrix.getRowCount()==bSegMatrix.getRowCount())&&(aSegMatrix.getColumnCount()==aSegMatrix.getRowCount())&&(bSegMatrix.getRowCount()==bSegMatrix.getColumnCount())){
                    double matrixKL=0;
                    double revMatrixKL=0;

                    for (int j = 0; j < aSegMatrix.getRowCount(); j++) {
                        for (int k = 0; k < aSegMatrix.getColumnCount(); k++) {
                            double tmpA=aSegMatrix.getAsDouble(j,k);
                            double tmpB=bSegMatrix.getAsDouble(j,k);
                            double replaceZero=Math.pow(2, -40);

                            if (tmpB!=0.0){
                                matrixKL+=tmpA*Math.log(tmpA/tmpB);
                            }
                            else{
                                matrixKL+=tmpA*Math.log(tmpA/replaceZero);
                            }

                            if (tmpA!=0.0){
                                revMatrixKL+=tmpB*Math.log(tmpB/tmpA);
                            }else{
                                revMatrixKL+=tmpB*Math.log(tmpB/replaceZero);
                            }
                        }
                    }
                    retValue+=(matrixKL+revMatrixKL)/(2*aSegMatrix.getColumnCount());
                }else {
                    System.out.println("size not compatible!");
                    return Double.MAX_VALUE;
                }
            }
        }
        return retValue;
    }


    public static void main(String[] args) {
        // TODO Auto-generated method stub

    }


}
