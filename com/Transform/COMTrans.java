package com.Transform;

import org.apache.commons.math3.complex.Complex;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * 此类用于Double类型的数据点进行转换
 * Created by hdp on 16-9-7.
 */
public class COMTrans implements Serializable {


    /**
     * 小波变换
     * @param flage 0表示做小波变换，1表示从小波恢复原来的数据
     * @param level 小波变换的层数
     * @param points 输入数据
     * @return
     */
    public ArrayList<Double> waveTrans(int flage,int level,ArrayList<Double> points) {
        if (flage==0) {
            return HaarCalculate(level,points);
        } else {
            return HaarRecovery(level,points);
        }
    }

    public ArrayList<Double> HaarCalculate(int waveLevel,ArrayList<Double> points) {
        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < waveLevel; i++) {
            int processLength=points.size()/(int)Math.pow(2,i);
            double[] high=new double[processLength/2];
            double[] low=new double[processLength/2];
            for (int j = 0; j < (processLength/2); j++) {
                low[j]=(points.get(j)+points.get(j+1))/2;
                high[j]=(points.get(j)-points.get(j+1))/2;
            }
            for (int j = 0; j < low.length; j++) {
                retValue.add(low[j]);
            }
            for (int j = 0; j < high.length; j++) {
                retValue.add(high[j]);
            }
        }
        return  retValue;
    }

    public ArrayList<Double> HaarRecovery(int waveLevel,ArrayList<Double> wPoints) {
        ArrayList<Double> points=new ArrayList<Double>();
        points.addAll(wPoints);
        for (int i = waveLevel; i >0; i--) {
            int proLength=points.size()/((int)Math.pow(2,i-1));
            double[] tmpRe=new double[proLength];
            for (int j = 0; j < proLength/2; j++) {
                tmpRe[j*2]=points.get(j)+points.get(proLength/2+j);
                tmpRe[j*2+1]=points.get(j)-points.get(proLength/2+j);
            }
            for (int j = 0; j < tmpRe.length; j++) {
                points.set(j, tmpRe[j]);
            }
        }
        return  points;
    }

    //获取小波变换第lth层的数据
    public ArrayList<Double> getHaarDataByLevel(int level,int lth,ArrayList<Double>x){
        ArrayList<Double> retValue=new ArrayList<Double>();
        if (level==0) {
            retValue.addAll(x);
        }else {
            int length=x.size();
            if (lth==0) {
                int subLength=(int)(length*Math.pow(0.5, level));
                for (int i=0;i<subLength;i++){
                    retValue.add(x.get(i));
                }
            }
            else {
                int begin=(int)(length*Math.pow(0.5, level-lth+1));
                int end=(int)(length*Math.pow(0.5, level-lth));
                for (int i = begin; i <end ; i++) {
                    retValue.add(x.get(i));
                }
            }
        }
        return  retValue;
    }

    //规范化数据
    public ArrayList<Double>  normalizing(int flag,ArrayList<Double> points){
        ArrayList<Double> retValue=new ArrayList<Double>();
        double norBase=0;
        if (flag==0) {//总和
            norBase=getSum(points);
        }
        else if (flag==1) {
            norBase=getMax(points);
        }


        if (norBase==0){
            retValue.addAll(points);
        }
        else{
            for (int i = 0; i <points.size(); i++) {
                retValue.add(points.get(i)/norBase);
            }
        }
        return retValue;
    }

    public double getMax(ArrayList<Double> points) {
        double retValue=0;
        for (int i = 0; i < points.size(); i++) {
            if (points.get(i)>retValue) {
                retValue=points.get(i);
            }
        }
        return retValue;
    }

    public double getMin(ArrayList<Double> points) {
        double retValue=Double.MAX_VALUE;
        for (int i = 0; i < points.size(); i++) {
            if (points.get(i)<retValue) {
                retValue=points.get(i);
            }
        }
        return retValue;
    }

    public double getSum(ArrayList<Double> points){
        double sum=0;
        for (int i = 0; i <points.size(); i++) {
            sum+=points.get(i);
        }
        return sum;
    }


    //规范化数据,通过平均值和标准差
    public ArrayList<Double> norByMeansAndVar(ArrayList<Double> points,ArrayList<Double> means,ArrayList<Double> var){
        ArrayList<Double> newPoints=new ArrayList<Double>();
        for (int i = 0; i < points.size(); i++) {
            newPoints.add((points.get(i)-means.get(i))/Math.sqrt(var.get(i)));
        }
        return newPoints;
    }

    // 以自身的均值以及标准差规范化
    public double[] zscore(double[] x){
        double mean=0;
        for (int i = 0; i < x.length; i++) {
            mean+=x[i];
        }
        mean=mean/x.length;

        double var=0;
        for (int i = 0; i < x.length; i++) {
            var+=Math.pow(x[i]-mean,2);
        }

        var=var/(x.length-1);

        double std=Math.sqrt(var);

        double[] retValue=new double[x.length];
        for (int i = 0; i < x.length; i++) {
            if(std!=0){
                retValue[i]=(x[i]-mean)/std;
            }else{
                retValue[i]=0.0;
            }

        }
        return retValue;
    }


    // 以自身的均值以及标准差规范化
    public ArrayList<Double> zscore(ArrayList<Double> x){
        double mean=0;
        for (int i = 0; i < x.size(); i++) {
            mean+=x.get(i);
        }
        mean=mean/x.size();

        double var=0;
        for (int i = 0; i < x.size(); i++) {
            var+=Math.pow(x.get(i)-mean,2);
        }

        //var=var/(x.size()-1);
        var=var/x.size();
        double std=Math.sqrt(var);

        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < x.size(); i++) {
            if(std!=0){
                retValue.add((x.get(i)-mean)/std);
            }else{
                retValue.add(0.0);
            }

        }
        return retValue;
    }

    //以最大值和最小值做归一化，x'=(x-xMin)/(xMax-xMin)
    public ArrayList<Double> norByMaxAndMin(ArrayList<Double> x){
        double minValue=getMin(x);
        double maxValue=getMax(x);
        ArrayList<Double> retValue=new ArrayList<Double>();
        double norBase=maxValue-minValue;
        if (maxValue==0.0){
            retValue.addAll(x);
        }
        else{
            for (int i = 0; i <x.size(); i++) {
                retValue.add((x.get(i)-minValue)/norBase);
            }
        }
        return retValue;
    }


    /**
     * SAX
     */
    public ArrayList<Double> sax(ArrayList<Double> data, ArrayList<ArrayList<Tuple2<Integer,Integer>>> xSeg,ArrayList<Tuple2<Double,Double>> ySeg){
        double[] newXSeg=new double[xSeg.size()];
        ArrayList<Double> points=new ArrayList<Double>();
        for (int i = 0; i < newXSeg.length; i++) {
            newXSeg[i]=0;
        }

        for (int i = 0; i < xSeg.size(); i++) {
            int dataWidth=0;
            ArrayList<Tuple2<Integer,Integer>> tmpSeg=xSeg.get(i);
            for (int j = 0; j < tmpSeg.size(); j++) {
                Tuple2<Integer,Integer> tmpTuple=tmpSeg.get(i);
                dataWidth+=(tmpTuple._2-tmpTuple._1+1);
                for (int k = tmpTuple._1; k < tmpTuple._2+1; k++) {
                    newXSeg[i]+=data.get(k);
                }
            }
            newXSeg[i]=newXSeg[i]/dataWidth;
        }

        for (int i = 0; i < newXSeg.length; i++) {
            if (ySeg.size()>0){
                for (int j = 0; j < ySeg.size(); j++) {
                    Tuple2<Double,Double> tmpYSeg=ySeg.get(i);
                    if ((newXSeg[i]>=tmpYSeg._1)&&(newXSeg[i]<tmpYSeg._2)){
                        points.add(new Double(j));
                    }
                }
            }else{
                points.add(newXSeg[i]);
            }
        }

        return points;
    }

    /**
     * FFT
     */
    public ArrayList<Double> FFT(ArrayList<Double> data){
        Complex[] dataComplex=new Complex[data.size()];
        Complex[] result=new Complex[data.size()];
        for (int i = 0; i <dataComplex.length; i++) {
            dataComplex[i]=new Complex(data.get(i),0);
        }
        result=fft(dataComplex);
        return getModel(result);
    }

    public ArrayList<Double> getModel(Complex[] data){
        ArrayList<Double> retValue=new ArrayList<Double>();
        for (int i = 0; i < data.length; i++) {
            retValue.add(data[i].abs());
        }
        return retValue;
    }

    public static Complex[] fft(Complex[] x) {

        int N = x.length;

        // base case
        if (N == 1) return new Complex[] { x[0] };

        // radix 2 Cooley-Tukey FFT
        if (N % 2 != 0) { throw new RuntimeException("N is not a power of 2"); }

        // fft of even terms
        Complex[] even = new Complex[N/2];
        for (int k = 0; k < N/2; k++) {
            even[k] = x[2*k];
        }
        Complex[] q = fft(even);

        // fft of odd terms
        Complex[] odd  = even;  // reuse the array
        for (int k = 0; k < N/2; k++) {
            odd[k] = x[2*k + 1];
        }
        Complex[] r = fft(odd);

        // combine
        Complex[] y = new Complex[N];
        for (int k = 0; k < N/2; k++) {
            double kth = -2 * k * Math.PI / N;
            Complex wk = new Complex(Math.cos(kth), Math.sin(kth));

            y[k]=q[k].add(wk.multiply(r[k]));
            y[k + N/2] = q[k].subtract(wk.multiply(r[k]));
        }
        return y;
    }

    public static void main(String[] args)  {
         double[] x={2,6,4,3,8,5};
        ArrayList<Double> y=new ArrayList<Double>();
        for (int i = 0; i < x.length; i++) {
            y.add(x[i]);
        }

        COMTrans ct=new COMTrans();
        double[] result=ct.zscore(x);
        ArrayList<Double> resultArray=ct.waveTrans(0,1,y);

        //for (int i = 0; i < result.length; i++) {
            //System.out.println(result[i]);
       // }

        for (int i = 0; i < resultArray.size(); i++) {
            System.out.println(resultArray.get(i));
        }
    }

}
