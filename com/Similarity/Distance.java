package com.Similarity;

import CommunityDetection.Shape_Recognition;
import JavaTuple.Tuple3IFP;
import com.Chart.LineChart;
import com.Data.DataOperation;
import com.Transform.FFT;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RefineryUtilities;
import org.ujmp.core.Matrix;

import java.awt.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Stack;

/**
 * 以距离表示两个Double类型的数据点之间的相似性
 * Created by hdp on 16-9-7.
 */
public class Distance implements Serializable {

    public double getEuclideanDistance(ArrayList<Double> a,ArrayList<Double> b){
        // TODO Auto-generated method stub
        double sum = 0.0;
        if(a.size() != b.size()) {
            System.out.println("size not compatible!");
            return Double.MAX_VALUE;
        }
        else{
            for(int i = 0;i < a.size();i++){
                sum += Math.pow(a.get(i).doubleValue() - b.get(i).doubleValue(), 2);
            }
            sum = Math.sqrt(sum);
        }
        return sum;
    }

    //pearson correlation dostance
    public double getPearsonCorrelationDistan(ArrayList<Double> a,ArrayList<Double> b){

        if(a.size() != b.size()) {
            System.out.println("size not compatible!");
            return Double.MAX_VALUE;
        }
        else{

            double cov=0;
            double aVar=0;
            double bVar=0;
            double aMeans=getMeans(a);
            double bMmeans=getMeans(b);

            for(int i = 0;i < a.size();i++){
                cov+=((a.get(i)-aMeans)*(b.get(i)-bMmeans));
                aVar+=Math.pow(a.get(i)-aMeans,2);
                bVar+=Math.pow(b.get(i)-bMmeans,2);
            }

            return 1-(cov/(Math.sqrt(aVar)*Math.sqrt(bVar)));
        }
    }

    public float getEuclideanDistanceForFloat(ArrayList<Float> a,ArrayList<Float> b){
        // TODO Auto-generated method stub
        float sum = 0;
        if(a.size() != b.size()) {
            System.out.println("size not compatible!");
            return Float.MAX_VALUE;
        }
        else{
            for(int i = 0;i < a.size();i++){
                sum += Math.pow(a.get(i) - b.get(i), 2);
            }
            sum = (float) Math.sqrt(sum);
        }
        return sum;
    }

    public double getEuclideanDistanceForMarkov(ArrayList<Matrix> a, ArrayList<Matrix> b){
        // TODO Auto-generated method stub
        double sum = 0.0;
        if(a.size() != b.size()) {
            System.out.println("size not compatible!");
            return Double.MAX_VALUE;
        }
        else{
            for(int i = 0;i < a.size();i++){
                if ((a.get(i).getRowCount()==b.get(i).getRowCount())&&(a.get(i).getColumnCount()==b.get(i).getColumnCount())){
                    for (int j = 0; j < a.get(i).getRowCount(); j++) {
                        for (int k = 0; k < a.get(i).getColumnCount(); k++) {
                            sum += Math.pow(a.get(i).getAsDouble(j,k) - b.get(i).getAsDouble(j,k), 2);
                        }
                    }
                }else {
                    System.out.println("size not compatible!");
                    return Double.MAX_VALUE;
                }
            }
            sum = Math.sqrt(sum);
        }
        return sum;
    }

    public double getPowDistanceForMarkov(ArrayList<Matrix> a){
        // TODO Auto-generated method stub
        double sum = 0.0;
            for(int i = 0;i < a.size();i++){
                    for (int j = 0; j < a.get(i).getRowCount(); j++) {
                        for (int k = 0; k < a.get(i).getColumnCount(); k++) {
                            sum += Math.pow(a.get(i).getAsDouble(j,k) , 2);
                        }
                    }
            }
        return sum;
    }

    public double getHammingDistance(ArrayList<Double> a,ArrayList<Double> b){
        // TODO Auto-generated method stub
        double sum = 0.0;
        if(a.size() != b.size()) {
            System.out.println("size not compatible!");
            return Double.MAX_VALUE;
        }
        else{
            for(int i = 0; i < a.size(); i++){
                if(!a.get(i).equals(b.get(i)))
                    sum = sum + 1;
            }
        }
        return sum;
    }

    public double getPowDistance(ArrayList<Double> a,ArrayList<Double> b) {
        double sum = 0.0;
        if ((a.size() != b.size()) && (b.size() != 0)) {
            System.out.println("size not compatible!");
            return Double.MAX_VALUE;
        }
        else if (b.size() == 0) {
            for (int i = 0; i < a.size(); i++) {
                sum += Math.pow(a.get(i).doubleValue(), 2);
            }
        } else {

            for (int i = 0; i < a.size(); i++) {
                sum += Math.pow(a.get(i).doubleValue() - b.get(i).doubleValue(), 2);
            }
        }
        return sum;
    }

    public static float getPowDistanceForFloat(ArrayList<Float> a,ArrayList<Float> b) {
        float sum = 0.0f;
        if ((a.size() != b.size()) && (b.size() != 0)) {
            System.out.println("size not compatible!");
            return Float.MAX_VALUE;
        }
        else if (b.size() == 0) {
            for (int i = 0; i < a.size(); i++) {
                sum += Math.pow(a.get(i).doubleValue(), 2);
            }
        } else {
            for (int i = 0; i < a.size(); i++) {
                sum += Math.pow(a.get(i).doubleValue() - b.get(i).doubleValue(), 2);
            }
        }
        return sum;
    }

    public double getDTWDistance(ArrayList<Double> a,ArrayList<Double> b){
        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    dpMatrix.setAsDouble(Math.pow(a.get(i)-b.get(j),2),i,j);
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
                    dpMatrix.setAsDouble(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+Math.pow(a.get(i)-b.get(j),2),i,j);
                }
            }
        }
        return Math.sqrt(dpMatrix.getAsDouble(a.size()-1,b.size()-1));
    }

    public double getDTWDistanceForDouble(ArrayList<Double> a,ArrayList<Double> b){
        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    dpMatrix.setAsDouble(Math.pow(a.get(i)-b.get(j),2),i,j);
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
                    dpMatrix.setAsDouble(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+Math.pow(a.get(i)-b.get(j),2),i,j);
                }
            }
        }
        return Math.sqrt(dpMatrix.getAsDouble(a.size()-1,b.size()-1));
    }

    public double getWDTWDistanceForDouble(ArrayList<Double> a,ArrayList<Double> b,double g){
        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    dpMatrix.setAsDouble(Math.pow((a.get(i)-b.get(j))*(1/(1+Math.exp(-g*(Math.abs(i-j)-(a.size()/2))))),2),i,j);
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
                    dpMatrix.setAsDouble(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+Math.pow((a.get(i)-b.get(j))*(1/(1+Math.exp(-g*(Math.abs(i-j)-(a.size()/2))))),2),i,j);
                }
            }
        }

        return Math.sqrt(dpMatrix.getAsDouble(a.size()-1,b.size()-1));
    }

    public double getWDTWDistanceForFloat(ArrayList<Float> a,ArrayList<Float> b,double g){
        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    dpMatrix.setAsFloat((float) Math.pow((a.get(i)-b.get(j))*(1/(1+Math.exp(-g*(Math.abs(i-j)-(a.size()/2))))),2),i,j);
                }else{
                    float iMinus2jMinus=Float.MAX_VALUE;
                    float iMinus2j=Float.MAX_VALUE;
                    float i2jMinus=Float.MAX_VALUE;
                    int indexiMinus=i-1;
                    int indexjMinus=j-1;

                    if ((indexiMinus>=0)&&(indexjMinus>=0)){
                        iMinus2jMinus=dpMatrix.getAsFloat(indexiMinus,indexjMinus);
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus<0)&&(indexjMinus>=0)){
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus>=0)&&(indexjMinus<0)){
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                    }
                    dpMatrix.setAsFloat(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+(float)Math.pow((a.get(i)-b.get(j))*(1/(1+Math.exp(-g*(Math.abs(i-j)-(a.size()/2))))),2),i,j);
                }
            }
        }


        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                System.out.print(dpMatrix.getAsFloat(i,j)+": "+Math.abs(a.get(i)-b.get(j))+"      ");
            }
            System.out.print("\n");
        }

        //return dpMatrix.getAsFloat(a.size()-1,b.size()-1)/(a.size()+b.size());
        ArrayList<Integer> align=new ArrayList<>();
        int x=a.size()-1;
        int y=b.size()-1;
        String retValue="";
        while (x>-1&&y>-1){
            retValue="\n"+x+"-"+y+" :  "+dpMatrix.getAsFloat(x,y)+"-"+(a.get(x)-b.get(y))+retValue;
            align.add(x);
            align.add(y);

            float x1=Float.MAX_VALUE;
            float x2=Float.MAX_VALUE;
            float x3=Float.MAX_VALUE;

            if (((x-1)>-1)&&((y-1)>-1)){
                x1=dpMatrix.getAsFloat(x-1,y-1);
            }
            if ((x-1)>-1){
                x2=dpMatrix.getAsFloat(x-1,y);
            }
            if ((y-1)>-1){
                x3=dpMatrix.getAsFloat(x,y-1);
            }

            float _min=x1;
            int _minStr=1;

            if (_min>x3){
                _min=x3;
                _minStr=3;
            }

            if (_min>x2){
                //_min=x2;
                _minStr=2;
            }


            if (_minStr==1){
                x=x-1;
                y=y-1;
            }else if (_minStr==2){
                x=x-1;
            }else {
                y=y-1;
            }

        }
        System.out.print(retValue);
        drawAlign(a,b,align,new HashSet<Integer>());

        return Math.sqrt(dpMatrix.getAsDouble(a.size()-1,b.size()-1));
    }

    public double getDTWDistanceForScalar(ArrayList<Integer> a,ArrayList<Integer> b,Matrix distMatrix){
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

        return dpMatrix.getAsDouble(a.size()-1,b.size()-1)/(a.size()+b.size());
    }

    public float getDTWDistanceForFloat(ArrayList<Float> a,ArrayList<Float> b){
        //Matrix euDistance=Matrix.Factory.zeros(a.size(),b.size());
        //for (int i = 0; i < a.size(); i++) {
           // for (int j = 0; j < b.size(); j++) {
               // euDistance.setAsFloat(Math.abs(a.get(i)-b.get(j)),i,j);
            //}
        //}

        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    //dpMatrix.setAsFloat(euDistance.getAsFloat(i,j),i,j);
                    dpMatrix.setAsFloat((float)Math.pow(a.get(i)-b.get(j),2),i,j);
                }else{
                    float iMinus2jMinus=Float.MAX_VALUE;
                    float iMinus2j=Float.MAX_VALUE;
                    float i2jMinus=Float.MAX_VALUE;
                    int indexiMinus=i-1;
                    int indexjMinus=j-1;

                    if ((indexiMinus>=0)&&(indexjMinus>=0)){
                        iMinus2jMinus=dpMatrix.getAsFloat(indexiMinus,indexjMinus);
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus<0)&&(indexjMinus>=0)){
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus>=0)&&(indexjMinus<0)){
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                    }
                    dpMatrix.setAsFloat(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+(float)Math.pow(a.get(i)-b.get(j),2),i,j);
                }
            }
        }

        return (float)Math.sqrt(dpMatrix.getAsFloat(a.size()-1,b.size()-1));
    }

    public ArrayList<ArrayList<Float>> getDTWMulAlignmentForFloat(ArrayList<Float> a,ArrayList<Float> b){

        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    dpMatrix.setAsFloat((float)Math.pow(a.get(i)-b.get(j),2),i,j);
                }else{
                    float iMinus2jMinus=Float.MAX_VALUE;
                    float iMinus2j=Float.MAX_VALUE;
                    float i2jMinus=Float.MAX_VALUE;
                    int indexiMinus=i-1;
                    int indexjMinus=j-1;

                    if ((indexiMinus>=0)&&(indexjMinus>=0)){
                        iMinus2jMinus=dpMatrix.getAsFloat(indexiMinus,indexjMinus);
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus<0)&&(indexjMinus>=0)){
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus>=0)&&(indexjMinus<0)){
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                    }
                    dpMatrix.setAsFloat(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+(float)Math.pow(a.get(i)-b.get(j),2),i,j);
                }
            }
        }

        ArrayList<ArrayList<Float>> b_mul_alignment=new ArrayList<ArrayList<Float>>();
        for (int i = 0; i < a.size(); i++) {
            b_mul_alignment.add(new ArrayList<Float>());
        }

        int x=a.size()-1;
        int y=b.size()-1;
        while ((x>-1)&&(y>-1)){
            b_mul_alignment.get(x).add(b.get(y));
            float x1=Float.MAX_VALUE;
            float x2=Float.MAX_VALUE;
            float x3=Float.MAX_VALUE;

            if (((x-1)>-1)&&((y-1)>-1)){
                x1=dpMatrix.getAsFloat(x-1,y-1);
            }
            if ((x-1)>-1){
                x2=dpMatrix.getAsFloat(x-1,y);
            }
            if ((y-1)>-1){
                x3=dpMatrix.getAsFloat(x,y-1);
            }

            float _min=x1;
            int _minStr=1;
            if (_min>x2){
                _min=x2;
                _minStr=2;
            }
            if (_min>x3){
                _minStr=3;
            }

            if (_minStr==1){
                x=x-1;
                y=y-1;
            }else if (_minStr==2){
                x=x-1;
            }else {
                y=y-1;
            }

        }
        return b_mul_alignment;
    }


    public double getPearsonCorrelationDistance(ArrayList<Double> a,ArrayList<Double> b){
        if (a.size()==b.size()){
            double abVar=0;
            double aVar=0;
            double bVar=0;
            double aMeans=getMeans(a);
            double bMeans=getMeans(b);
            for (int i = 0; i < a.size(); i++) {
                abVar+=(a.get(i)-aMeans)*(b.get(i)-bMeans);
                aVar+=Math.pow(a.get(i)-aMeans,2);
                bVar+=Math.pow(b.get(i)-bMeans,2);
            }
            return abVar/(Math.sqrt(aVar)*Math.sqrt(bVar));
        }else{
            return Double.MAX_VALUE;
        }
    }

    public double getMeans(ArrayList<Double> a){
        double sumData=0;
        for (int i = 0; i < a.size(); i++) {
            sumData+=a.get(i);
        }
        if (a.size()!=0){
            return sumData/a.size();
        }else{
            return 0;
        }
    }

    public float getDTWDistanceForFloatPrint(ArrayList<Float> a,ArrayList<Float> b){
        //Matrix euDistance=Matrix.Factory.zeros(a.size(),b.size());
        //for (int i = 0; i < a.size(); i++) {
        // for (int j = 0; j < b.size(); j++) {
        // euDistance.setAsFloat(Math.abs(a.get(i)-b.get(j)),i,j);
        //}
        //}

        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    //dpMatrix.setAsFloat(euDistance.getAsFloat(i,j),i,j);
                    dpMatrix.setAsFloat((float) Math.pow(a.get(i)-b.get(j),2),i,j);
                }else{
                    float iMinus2jMinus=Float.MAX_VALUE;
                    float iMinus2j=Float.MAX_VALUE;
                    float i2jMinus=Float.MAX_VALUE;
                    int indexiMinus=i-1;
                    int indexjMinus=j-1;

                    if ((indexiMinus>=0)&&(indexjMinus>=0)){
                        iMinus2jMinus=dpMatrix.getAsFloat(indexiMinus,indexjMinus);
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus<0)&&(indexjMinus>=0)){
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus>=0)&&(indexjMinus<0)){
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                    }
                    dpMatrix.setAsFloat(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+(float) Math.pow(a.get(i)-b.get(j),2),i,j);
                }
            }
        }

        //for (int i = 0; i < a.size(); i++) {
            //for (int j = 0; j < b.size(); j++) {
                //System.out.print(dpMatrix.getAsFloat(i,j)+": "+Math.abs(a.get(i)-b.get(j))+"      ");
            //}
            //System.out.print("\n");
        //}

        //return dpMatrix.getAsFloat(a.size()-1,b.size()-1)/(a.size()+b.size());
        return (float)Math.sqrt(dpMatrix.getAsFloat(a.size()-1,b.size()-1));

    }

    public float calDist(ArrayList<Float> a,ArrayList<Float> b,int iA,int iB,ArrayList<Integer> spaceA,int spaceMeansA,ArrayList<Integer> spaceB, int spaceMeansB,double g){
        int _spaceA=spaceMeansA;
        for (int i = 0; i < spaceA.size()/2; i++) {
            if (i==spaceA.size()/2-1){
                if ((spaceA.get(2*i)<=iA)&&(spaceA.get(2*i+1)>=iA)){
                    _spaceA=spaceA.get(2*i+1)-spaceA.get(2*i);
                    break;
                }
            }else {
                if ((spaceA.get(2*i)<=iA)&&(spaceA.get(2*i+1)>iA)){
                    _spaceA=spaceA.get(2*i+1)-spaceA.get(2*i);
                    break;
                }
            }
        }

        int _spaceB=spaceMeansB;
        for (int i = 0; i < spaceB.size()/2; i++) {
            if (i==spaceB.size()/2-1){
                if ((spaceB.get(2*i)<=iB)&&(spaceB.get(2*i+1)>=iB)){
                    _spaceB=spaceB.get(2*i+1)-spaceB.get(2*i);
                    break;
                }
            }else {
                if ((spaceB.get(2*i)<=iB)&&(spaceB.get(2*i+1)>iB)){
                    _spaceB=spaceB.get(2*i+1)-spaceB.get(2*i);
                    break;
                }
            }
        }

        int space=(_spaceA+_spaceB)/2;

        int phase=Math.abs(iA-iB);

        double pDistan=(Math.pow(a.get(iA)-b.get(iA),2)+Math.pow(a.get(iB)-b.get(iB),2))/2;
        //double pDistan=Math.sqrt(Math.pow(a.get(iA)-b.get(iA),2)+Math.pow(a.get(iB)-b.get(iB),2))/2;
        //double pDistan=Math.abs(a.get(iA)-b.get(iB));
        if (space==0){
            //return (float) (pDistan*(1/(1+Math.exp(-10*((phase/(space+0.00001)/(a.size()*0.01))-0.5)))));
            //return (float) Math.pow(pDistan*(1/(1+Math.exp(-10*((phase/(space+0.00001))-1)))),2);
            //return (float) (pDistan*(1/(1+Math.exp(-15*((phase/(space+0.00001))*((4/spaceA.size()+spaceB.size()))-0.85)))));
            return (float) (pDistan*(1/(1+Math.exp(-15*(1-g)))));
        }else {
            //return (float) (pDistan*(1/(1+Math.exp(-10*((phase/space/(a.size()*0.01))-0.5)))));
            //return (float) Math.pow(pDistan*(1/(1+Math.exp(-10*((phase/space)-1)))),2);
            //return (float) (pDistan*(1/(1+Math.exp(-15*((phase/space)*((4/spaceA.size()+spaceB.size()))-0.85)))));
            return (float) (pDistan*(1/(1+Math.exp(-15*((phase/space)-g)))));
        }
    }

    public int getSpaceMeans(ArrayList<Integer> spaceA){
        int retValue=0;
        for (int i = 0; i <spaceA.size()/2 ; i++) {
            retValue+=(spaceA.get(2*i+1)-spaceA.get(2*i));
        }
        if (spaceA.size()>0){
            return retValue/(spaceA.size()/2);
        }else {
            return retValue;
        }

    }

    public boolean isLine(ArrayList<Float> data){
        for (int i = 1; i < data.size(); i++) {
            if (data.get(i)!=data.get(i-1)){
                return false;
            }
        }
        return true;
    }

    public float get_Shift_Punishment_DTWDistanceFor(ArrayList<Float> a,ArrayList<Float> b,double g){

        ArrayList<Integer> spaceA=new ArrayList<Integer>();
        ArrayList<Integer> spaceB=new ArrayList<Integer>();

        if (!isLine(a)){
            spaceA=Shape_Recognition.getBreakpoint(a);
        }
        if (!isLine(b)){
            spaceB=Shape_Recognition.getBreakpoint(b);
        }

        int spaceMeansB=getSpaceMeans(spaceB);
        int spaceMeansA=getSpaceMeans(spaceA);

        Matrix dpMatrix=Matrix.Factory.zeros(a.size(),b.size());
        for (int i = 0; i < a.size(); i++) {
            for (int j = 0; j < b.size(); j++) {
                if ((j==0)&&(i==0)){
                    //dpMatrix.setAsFloat(euDistance.getAsFloat(i,j),i,j);
                    dpMatrix.setAsFloat((float)Math.pow(a.get(i)-b.get(j),2),i,j);
                }else{
                    float iMinus2jMinus=Float.MAX_VALUE;
                    float iMinus2j=Float.MAX_VALUE;
                    float i2jMinus=Float.MAX_VALUE;
                    int indexiMinus=i-1;
                    int indexjMinus=j-1;

                    if ((indexiMinus>=0)&&(indexjMinus>=0)){
                        iMinus2jMinus=dpMatrix.getAsFloat(indexiMinus,indexjMinus);
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus<0)&&(indexjMinus>=0)){
                        i2jMinus=dpMatrix.getAsFloat(i,indexjMinus);
                    }else if ((indexiMinus>=0)&&(indexjMinus<0)){
                        iMinus2j=dpMatrix.getAsFloat(indexiMinus,j);
                    }
                    dpMatrix.setAsFloat(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+(float)(Math.pow(a.get(i)-b.get(j),2)+calDist(a,b,i,j,spaceA,spaceMeansA,spaceB,spaceMeansB,g)),i,j);
                    //dpMatrix.setAsFloat(Math.min(Math.min(iMinus2jMinus,i2jMinus),iMinus2j)+calDist(a,b,i,j,spaceA,spaceMeansA,spaceB,spaceMeansB),i,j);
                }
            }
        }

        //region 输出动态规划矩阵
        /*
        for (int i = 0; i < a.size(); i++) {
        for (int j = 0; j < b.size(); j++) {
        System.out.print(dpMatrix.getAsFloat(i,j)+": "+Math.abs(a.get(i)-b.get(j))+"      ");
        }
        System.out.print("\n");
        }
        */
        //endregion

        //region 输出DTW路径
        /*
        Stack<Integer> path_x=new Stack<Integer>();
        Stack<Integer> path_y=new Stack<Integer>();
        String retValue="";
        int x=a.size()-1;
        int y=b.size()-1;
        while (x>-1&&y>-1){
            path_x.add(x);
            path_y.add(y);
            retValue="\n"+x+"-"+y+" :  "+dpMatrix.getAsFloat(x,y)+"-"+(a.get(x)-b.get(y))+retValue;
            float x1=Float.MAX_VALUE;
            float x2=Float.MAX_VALUE;
            float x3=Float.MAX_VALUE;

            if (((x-1)>-1)&&((y-1)>-1)){
                x1=dpMatrix.getAsFloat(x-1,y-1);
            }
            if ((x-1)>-1){
                x2=dpMatrix.getAsFloat(x-1,y);
            }
            if ((y-1)>-1){
                x3=dpMatrix.getAsFloat(x,y-1);
            }

            float _min=x1;
            int _minStr=1;
            if (_min>x2){
                _min=x2;
                _minStr=2;
            }
            if (_min>x3){
                _minStr=3;
            }

            if (_minStr==1){
                x=x-1;
                y=y-1;
            }else if (_minStr==2){
                x=x-1;
            }else {
                y=y-1;
            }

        }
        System.out.print(retValue);
        */
        //endregion
        return (float)Math.sqrt(dpMatrix.getAsFloat(a.size()-1,b.size()-1));

    }

    public void drawAlign(ArrayList<Float> x, ArrayList<Float> y, ArrayList<Integer> align,HashSet<Integer> ex){

        XYSeriesCollection sc=new XYSeriesCollection();
        XYSeries s1 = new XYSeries("x");
        XYSeries s2 = new XYSeries("y");

        for (int i = 0; i < x.size(); i++) {
            s1.add(i,x.get(i)+2);
        }

        for (int i = 0; i < y.size(); i++) {
            s2.add(i,y.get(i));
        }


        sc.addSeries(s1);
        sc.addSeries(s2);

        for (int i = 0; i < align.size()/2; i++) {
            XYSeries tmp=new XYSeries(align.get(2*i)+"-"+align.get(2*i+1));
            double xtmp=x.get(align.get(2*i))+2.0;
            tmp.add((double)align.get(2*i),xtmp);
            tmp.add(align.get(2*i+1),y.get(align.get(2*i+1)));
            sc.addSeries(tmp);
        }

        LineChart lChart=new LineChart("Electricity consumption");
        XYDataset dataSet=sc;
        ArrayList<Tuple3IFP> style=new ArrayList<Tuple3IFP>();
        style.add(new Tuple3IFP(1,5f,Color.RED));
        style.add(new Tuple3IFP(1,5f,Color.BLUE));

        for (int i = 2; i < dataSet.getSeriesCount(); i++) {
            if ((ex.size()>0)&&(ex.contains(i))){
                style.add(new Tuple3IFP(0,0.5f, Color.RED));
            }else {
                style.add(new Tuple3IFP(0,0.5f, Color.BLACK));
            }

        }

        // lChart.drawLine2D("测试", "xxxx", "people", dataset, flag, true);
        //lChart.drawLine2DXY(cluNum,"No clusters","Assessment value",dataSet,style);
        lChart.drawLine2DXY("DTW","Time/Quarter","Consumption/KWh",dataSet,style,-1);
        lChart.pack( );
        RefineryUtilities.centerFrameOnScreen( lChart );
        lChart.setVisible( true );
    }

    public static void main(String[] args)  {

        String data1="1412341,2016-09-22,1,kjhhjkh,2016-09-22,96,1111111111111111111111111111111,11.071972228754136,14.224958822363982,9.061118880651964,19.079622344926527,12.348726178826468,15.554602621759697,13.435097299164266,15.671636201797927,0.996779651995412,6.2179278593122245,0.37825936139666894,0.9505862580824154,11.191332144577537,4.127758000753234,4.9279017054527845,19.203638721795514,8.696689348181845,17.12247803931095,14.639544491605909,10.119835751780615,10.553882999586646,19.22916807622675,9.591908999537518,10.802182891111006,6.034869002368939,2.348741115353068,2.02424942842097,18.015268861456246,5.306781607765729,15.241130189615177,10.61398424448278,6.814057354946678,3.909879643658536,6.0049878173012905,7.625207529942466,10.078383348210288,0.9379814499889805,1.6877612412816534,18.506330103978698,14.981593963847734,6.720339673661382,9.228409901904222,12.987759007110851,7.050687374980667,1.8390586743230242,5.626450545517243,4.328167198726176,10.229201827029726,3.4763191073561672,2.0474005054489686,9.080576524814362,12.233012889293866,17.37083018871391,16.516121179079228,13.597117756741481,8.493113825466992,4.073083784163158,12.915948878144079,5.464005559073961,5.108349002410259,6.724137591201822,0.20291802577729445,18.1559059637785,11.48497215052813,14.24282338864776,15.060783535225966,5.421538616197548,6.718833292784662,8.59141740328677,13.663957483423356,8.260011144337616,0.27987116380016497,7.699976631172786,13.7818812449872,1.8200451765104741,7.18751370429538,6.66469453785157,17.149756290596276,12.151888222540567,0.619497405920908,18.99579174925154,3.846972759475966,13.464394498959711,10.775509579431677,6.975610377557004,5.365968032037902,15.387387272003501,7.820973874190276,5.5977343388159895,11.12066613533213,6.712079887165263,18.865202826321198,8.093120457964552,4.335049101468447,17.179997481835226,4.182047055675559";
        String data2="1412341,2016-09-22,1,kjhhjkh,2016-09-22,96,1111111111111111111111111111111,11.071972228754136,14.224958822363982,9.061118880651964,19.079622344926527,12.348726178826468,15.554602621759697,13.435097299164266,15.671636201797927,0.996779651995412,6.2179278593122245,0.37825936139666894,0.9505862580824154,11.191332144577537,4.127758000753234,4.9279017054527845,19.203638721795514,8.696689348181845,17.12247803931095,14.639544491605909,10.119835751780615,10.553882999586646,19.22916807622675,9.591908999537518,10.802182891111006,6.034869002368939,2.348741115353068,2.02424942842097,18.015268861456246,5.306781607765729,15.241130189615177,10.61398424448278,6.814057354946678,3.909879643658536,6.0049878173012905,7.625207529942466,10.078383348210288,0.9379814499889805,1.6877612412816534,18.506330103978698,14.981593963847734,6.720339673661382,9.228409901904222,12.987759007110851,7.050687374980667,1.8390586743230242,5.626450545517243,4.328167198726176,10.229201827029726,3.4763191073561672,2.0474005054489686,9.080576524814362,12.233012889293866,17.37083018871391,16.516121179079228,13.597117756741481,8.493113825466992,4.073083784163158,12.915948878144079,5.464005559073961,5.108349002410259,6.724137591201822,0.20291802577729445,18.1559059637785,11.48497215052813,14.24282338864776,15.060783535225966,5.421538616197548,6.718833292784662,8.59141740328677,13.663957483423356,8.260011144337616,0.27987116380016497,7.699976631172786,13.7818812449872,1.8200451765104741,7.18751370429538,6.66469453785157,17.149756290596276,12.151888222540567,0.619497405920908,18.99579174925154,3.846972759475966,13.464394498959711,10.775509579431677,6.975610377557004,5.365968032037902,15.387387272003501,7.820973874190276,5.5977343388159895,11.12066613533213,6.712079887165263,18.865202826321198,8.093120457964552,4.335049101468447,17.179997481835226,4.182047055675559";
        //x=[5,5,4,4,3,3,9]
        //y=[4,4,4,4,2,2,2]

        //String a="1,1,1,6,7,8,9,6,1,1,1,0,0,0,0,0,1,1,2,1,1";
        //String b="2,1,1,2,1,1,0,1,0,1,0,0,0,6,7,8,9,6,1,0,1";

        String a="0,0,0,0,0,0,1,3,5,6,7.5,8.3,9,8.3,7.5,6,5,3,1,0,0,0,5,6,7,8,7,6,5,0,0,0,0,0";
        String b="0,0,0,1,3,5,6,7.5,8.3,9,8.3,7.5,6,5,3,1,0,0,0,0,0,0,0,0,0,5,6,7,8,7,6,5,0,0";

        ArrayList<Float> x=new ArrayList<Float>();
        ArrayList<Float> y=new ArrayList<Float>();




        XYSeriesCollection sc=new XYSeriesCollection();
        XYSeries s1 = new XYSeries("x");
        XYSeries s2 = new XYSeries("y");

        String[] aa=a.split(",");
        for (int i = 0; i < aa.length; i++) {
            x.add(Float.parseFloat(aa[i]));
            s1.add(i,Double.parseDouble(aa[i])+1);
        }

        String[] bb=b.split(",");
        for (int i = 0; i < bb.length; i++) {
            y.add(Float.parseFloat(bb[i]));
            s2.add(i,Double.parseDouble(bb[i]));
        }
        sc.addSeries(s1);
        sc.addSeries(s2);
        LineChart lChart=new LineChart("Electricity consumption");
        XYDataset dataSet=sc;
        ArrayList<Tuple3IFP> style=new ArrayList<Tuple3IFP>();
        for (int i = 0; i < dataSet.getSeriesCount(); i++) {
            style.add(new Tuple3IFP(1,5f,null));
        }

        // lChart.drawLine2D("测试", "xxxx", "people", dataset, flag, true);
        //lChart.drawLine2DXY(cluNum,"No clusters","Assessment value",dataSet,style);
        lChart.drawLine2DXY("DTW","Time/Quarter","Consumption/KWh",dataSet,style,-1);
        lChart.pack( );
        RefineryUtilities.centerFrameOnScreen( lChart );
        lChart.setVisible( true );

        Distance dsit=new Distance();
        //double kk=dsit.getDTWDistance(e1.getPoints(),e2.getPoints());
        double kk=dsit.getWDTWDistanceForFloat(x,y,0.4);

        System.out.println("\n\nWDTW value:"+kk);
        System.out.println("\n\nDTW value:"+dsit.getDTWDistanceForFloat(x,y));
        System.out.println("\n\nEU value:"+dsit.getEuclideanDistanceForFloat(x,y));
        System.out.println("\n\nSDB value:"+ FFT.getSDB(DataOperation.Float2Doubel(x),DataOperation.Float2Doubel(y))._2);
        System.out.println("\n\nSWDTW value:"+ dsit.get_Shift_Punishment_DTWDistanceFor(x,y,0.85));
    }
}
