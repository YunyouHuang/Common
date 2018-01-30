package com.Transform;

import com.Data.JavaTupleForIntegerDoble;
import scala.Tuple2;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
/**
 * 快速傅里叶变换，以及卷积计算
 * Created by hdp on 16-10-4.
 */
public class FftConv implements Serializable {
    /*
     * Computes the discrete Fourier transform (DFT) of the given complex vector, storing the result back into the vector.
     * The vector can have any length. This is a wrapper function.
     */
    public static void transform(double[] real, double[] imag) {
        if (real.length != imag.length)
            throw new IllegalArgumentException("Mismatched lengths");

        int n = real.length;
        if (n == 0)
            return;
        else if ((n & (n - 1)) == 0)  // Is power of 2
            transformRadix2(real, imag);
        else  // More complicated algorithm for arbitrary sizes
            transformBluestein(real, imag);
    }


    /*
     * Computes the inverse discrete Fourier transform (IDFT) of the given complex vector, storing the result back into the vector.
     * The vector can have any length. This is a wrapper function. This transform does not perform scaling, so the inverse is not a true inverse.
     */
    public static void inverseTransform(double[] real, double[] imag) {
        transform(imag, real);
    }


    /*
     * Computes the discrete Fourier transform (DFT) of the given complex vector, storing the result back into the vector.
     * The vector's length must be a power of 2. Uses the Cooley-Tukey decimation-in-time radix-2 algorithm.
     */
    public static void transformRadix2(double[] real, double[] imag) {
        // Initialization
        if (real.length != imag.length)
            throw new IllegalArgumentException("Mismatched lengths");
        int n = real.length;
        int levels = 31 - Integer.numberOfLeadingZeros(n);  // Equal to floor(log2(n))
        if (1 << levels != n)
            throw new IllegalArgumentException("Length is not a power of 2");
        double[] cosTable = new double[n / 2];
        double[] sinTable = new double[n / 2];
        for (int i = 0; i < n / 2; i++) {
            cosTable[i] = Math.cos(2 * Math.PI * i / n);
            sinTable[i] = Math.sin(2 * Math.PI * i / n);
        }

        // Bit-reversed addressing permutation
        for (int i = 0; i < n; i++) {
            int j = Integer.reverse(i) >>> (32 - levels);
            if (j > i) {
                double temp = real[i];
                real[i] = real[j];
                real[j] = temp;
                temp = imag[i];
                imag[i] = imag[j];
                imag[j] = temp;
            }
        }

        // Cooley-Tukey decimation-in-time radix-2 FFT
        for (int size = 2; size <= n; size *= 2) {
            int halfsize = size / 2;
            int tablestep = n / size;
            for (int i = 0; i < n; i += size) {
                for (int j = i, k = 0; j < i + halfsize; j++, k += tablestep) {
                    double tpre =  real[j+halfsize] * cosTable[k] + imag[j+halfsize] * sinTable[k];
                    double tpim = -real[j+halfsize] * sinTable[k] + imag[j+halfsize] * cosTable[k];
                    real[j + halfsize] = real[j] - tpre;
                    imag[j + halfsize] = imag[j] - tpim;
                    real[j] += tpre;
                    imag[j] += tpim;
                }
            }
            if (size == n)  // Prevent overflow in 'size *= 2'
                break;
        }
    }


    /*
     * Computes the discrete Fourier transform (DFT) of the given complex vector, storing the result back into the vector.
     * The vector can have any length. This requires the convolution function, which in turn requires the radix-2 FFT function.
     * Uses Bluestein's chirp z-transform algorithm.
     */
    public static void transformBluestein(double[] real, double[] imag) {
        // Find a power-of-2 convolution length m such that m >= n * 2 + 1
        if (real.length != imag.length)
            throw new IllegalArgumentException("Mismatched lengths");
        int n = real.length;
        if (n >= 0x20000000)
            throw new IllegalArgumentException("Array too large");
        int m = Integer.highestOneBit(n * 2 + 1) << 1;

        // Trignometric tables
        double[] cosTable = new double[n];
        double[] sinTable = new double[n];
        for (int i = 0; i < n; i++) {
            int j = (int)((long)i * i % (n * 2));  // This is more accurate than j = i * i
            cosTable[i] = Math.cos(Math.PI * j / n);
            sinTable[i] = Math.sin(Math.PI * j / n);
        }

        // Temporary vectors and preprocessing
        double[] areal = new double[m];
        double[] aimag = new double[m];
        for (int i = 0; i < n; i++) {
            areal[i] =  real[i] * cosTable[i] + imag[i] * sinTable[i];
            aimag[i] = -real[i] * sinTable[i] + imag[i] * cosTable[i];
        }
        double[] breal = new double[m];
        double[] bimag = new double[m];
        breal[0] = cosTable[0];
        bimag[0] = sinTable[0];
        for (int i = 1; i < n; i++) {
            breal[i] = breal[m - i] = cosTable[i];
            bimag[i] = bimag[m - i] = sinTable[i];
        }

        // Convolution
        double[] creal = new double[m];
        double[] cimag = new double[m];
        convolve(areal, aimag, breal, bimag, creal, cimag);

        // Postprocessing
        for (int i = 0; i < n; i++) {
            real[i] =  creal[i] * cosTable[i] + cimag[i] * sinTable[i];
            imag[i] = -creal[i] * sinTable[i] + cimag[i] * cosTable[i];
        }
    }


    /*
     * Computes the circular convolution of the given real vectors. Each vector's length must be the same.
     */
    public static void convolve(double[] x, double[] y, double[] out) {
        if (x.length != y.length || x.length != out.length)
            throw new IllegalArgumentException("Mismatched lengths");
        int n = x.length;
        convolve(x, new double[n], y, new double[n], out, new double[n]);
    }


    /*
     * Computes the circular convolution of the given complex vectors. Each vector's length must be the same.
     */
    public static void convolve(double[] xreal, double[] ximag, double[] yreal, double[] yimag, double[] outreal, double[] outimag) {
        if (xreal.length != ximag.length || xreal.length != yreal.length || yreal.length != yimag.length || xreal.length != outreal.length || outreal.length != outimag.length)
            throw new IllegalArgumentException("Mismatched lengths");

        int n = xreal.length;
        xreal = xreal.clone();
        ximag = ximag.clone();
        yreal = yreal.clone();
        yimag = yimag.clone();

        transform(xreal, ximag);
        transform(yreal, yimag);
        for (int i = 0; i < n; i++) {
            double temp = xreal[i] * yreal[i] - ximag[i] * yimag[i];
            ximag[i] = ximag[i] * yreal[i] + xreal[i] * yimag[i];
            xreal[i] = temp;
        }
        inverseTransform(xreal, ximag);
        for (int i = 0; i < n; i++) {  // Scaling (because this FFT implementation omits it)
            outreal[i] = xreal[i] / n;
            outimag[i] = ximag[i] / n;
        }
    }


    //计算序列间最大的CC值及位置//<平移单位数，相似度>
    public static Tuple2<Integer,Double> getMaxNNCc(ArrayList<Double> centers, ArrayList<Double> x){
        if (centers.size()==x.size()){
            int arrayLength=x.size()*2-1;
            double[] xreal=new double[arrayLength];
            double[] ximag=new double[arrayLength];
            double[] yreal=new double[arrayLength];
            double[] yimag=new double[arrayLength];
            double[] outreal=new double[arrayLength];
            double[] outimag=new double[arrayLength];

            for (int i = 0; i < arrayLength; i++) {
                if (i<x.size()){
                    xreal[i]=centers.get(i);
                    yreal[i]=x.get(i);
                }else {
                    xreal[i]=0;
                    yreal[i]=0;
                }
                ximag[i]=0;
                yimag[i]=0;
            }

            getCrossCorrelation(xreal,ximag,yreal,yimag,outreal,outimag);

            int maxIndex=-1;
            double maxValue=-Double.MAX_VALUE;
            double absT=getNNCcNor(centers,x);
            for (int i = 0; i <outreal.length ; i++) {
                double tmpValue=outreal[i]/absT;
                if (tmpValue>maxValue){
                    maxIndex=i;
                    maxValue=tmpValue;
                }
            }
            return new Tuple2<Integer, Double>(maxIndex+1,maxValue);
            //return new JavaTupleForIntegerDoble(maxIndex+1,maxValue);
        }else {
            return new Tuple2<Integer, Double>(-1,0.0);
        }
    }


    //计算序列间最大的CC值及位置
    public static JavaTupleForIntegerDoble getMaxNNCcForJavaTu(ArrayList<Double> centers, ArrayList<Double> x){
        if (centers.size()!=x.size()){
            int arrayLength=x.size()*2-1;
            double[] xreal=new double[arrayLength];
            double[] ximag=new double[arrayLength];
            double[] yreal=new double[arrayLength];
            double[] yimag=new double[arrayLength];
            double[] outreal=new double[arrayLength];
            double[] outimag=new double[arrayLength];

            for (int i = 0; i < arrayLength; i++) {
                if (i<x.size()){
                    xreal[i]=centers.get(i);
                    yreal[i]=x.get(i);
                }else {
                    xreal[i]=0;
                    yreal[i]=0;
                }
                ximag[i]=0;
                yimag[i]=0;
            }

            getCrossCorrelation(xreal,ximag,yreal,yimag,outreal,outimag);

            int maxIndex=-1;
            double maxValue=0;
            for (int i = 0; i <outreal.length ; i++) {
                double tmpValue=outreal[i]/getNNCcNor(centers,x);
                if (tmpValue>maxValue){
                    maxIndex=i;
                    maxValue=tmpValue;
                }
            }
            return new JavaTupleForIntegerDoble(maxIndex+1,maxValue);
        }else {
            return new JavaTupleForIntegerDoble(-1,0.0);
        }
    }


    public static double getNNCcNor(ArrayList<Double> centers,ArrayList<Double> x){
        double tmpCen=0;
        double tmpX=0;
        for (int i = 0; i < centers.size(); i++) {
            tmpCen+=centers.get(i)*centers.get(i);
            tmpX+=x.get(i)*x.get(i);
        }
        return Math.sqrt(tmpCen*tmpX);
    }


    public static void getCrossCorrelation(double[] xreal, double[] ximag, double[] yreal, double[] yimag, double[] outreal, double[] outimag) {
        //transformBluestein(ximag, ximag);

        reverseArray(yreal);
        reverseArray(yimag);
        transformBluestein(xreal, ximag);
        transformBluestein(yreal, yimag);

        //for (int i = 0; i < ximag.length; i++) {
        //ximag[i]=-ximag[i];//复数取共轭
        //}
        for (int i = 0; i < xreal.length; i++) {
            outreal[i]=xreal[i]*yreal[i]-ximag[i]*yimag[i];
            outimag[i]=-(ximag[i]*yreal[i]+xreal[i]*yimag[i]);//先计算序列的虚部，然后取共轭，为下一步逆傅里叶变换做准备
        }
        transformBluestein(outreal, outimag);
        for (int i = 0; i < outimag.length; i++) {
            outimag[i]=(-outimag[i])/outimag.length;//取共轭，再除于变换序列的长度
            outreal[i]=outreal[i]/outreal.length;
        }
        DecimalFormat df = new DecimalFormat("#.#####");
        for (int i = 0; i < outreal.length; i++) {
            outreal[i]=Double.parseDouble(df.format(outreal[i]));
            outimag[i]=Double.parseDouble(df.format(outimag[i]));
        }
    }


    public static void reverseArray(double[]x) {
        int reLenght=(x.length+1)/2;
        double[]tmp=new double[reLenght];
        for (int i = 0; i < reLenght; i++) {
            tmp[i]=x[reLenght-1-i];
        }
        for (int i = 0; i < reLenght; i++) {
            x[i]=tmp[i];
        }
    }


    public static void getCCFft2(double[] xreal, double[] ximag, double[] yreal, double[] yimag, double[] outreal, double[] outimag) {

        //transformBluestein(ximag, ximag);
        transformBluestein(xreal, ximag);
        transformBluestein(yreal, yimag);

        //for (int i = 0; i < ximag.length; i++) {
        //ximag[i]=-ximag[i];//复数取共轭
        //}
        for (int i = 0; i < xreal.length; i++) {
            outreal[i]=xreal[i]*yreal[i]-ximag[i]*yimag[i];
            outimag[i]=-(ximag[i]*yreal[i]+xreal[i]*yimag[i]);//先计算序列的虚部，然后取共轭，为下一步逆傅里叶变换做准备
        }
        transformBluestein(outreal, outimag);
        for (int i = 0; i < outimag.length; i++) {
            outimag[i]=(-outimag[i])/outimag.length;//取共轭，再除于变换序列的长度
            outreal[i]=outreal[i]/outreal.length;
        }
        DecimalFormat df = new DecimalFormat("#.#####");
        for (int i = 0; i < outreal.length; i++) {
            outreal[i]=Double.parseDouble(df.format(outreal[i]));
            outimag[i]=Double.parseDouble(df.format(outimag[i]));
        }

    }


    public static void getCC2(double[] xreal, double[] yreal, double[] outreal) {
        int ccLength=xreal.length*2-1;
        for (int i = 0; i < ccLength; i++) {
            outreal[i]=getRcc(xreal, yreal, i+1-xreal.length);
        }
        DecimalFormat df = new DecimalFormat("#.#####");
        for (int i = 0; i < outreal.length; i++) {
            outreal[i]=Double.parseDouble(df.format(outreal[i]));
        }
    }


    public static double getRcc(double[] xreal, double[] yreal,int k) {
        double retVlaue=0;
        int xLengt=xreal.length;
        if (k>=0) {
            double tmpSum=0;
            for (int i = 0; i < xLengt-k; i++) {
                tmpSum+=xreal[i+k]*yreal[i];
            }
            retVlaue=tmpSum;
        }
        else {
            retVlaue=getRcc(yreal, xreal, -k);
        }
        return retVlaue;
    }


    public static void main(String[] args) {
        double[]a={1,5,6,5,0,0,0};
        double[]b={1.2,5.1,0.6,3.5,0,0,0};
        double[]c={0,0,0,0,0,0,0};
        double[]d={0,0,0,0,0,0,0};
        double[]outR=new double[7];
        double[]outI=new double[7];

        double[]a2={1,5,6,5};
        double[]b2={1.2,5.1,0.6,3.5};
        double[]c2=new double[7];

        FftConv ft=new FftConv();

        //ft.transformBluestein(a, c);
        //System.out.println("\n\n");
        //ft.transformBluestein(a, c);
        //ft.transformBluestein(b, d);
        ft.getCrossCorrelation(a, c, b, d, outR, outI);
        ft.getCC2(a2, b2, c2);

        //for (int i = 0; i < outI.length; i++) {
        //outI[i]=-outI[i];//先计算序列的虚部，然后取共轭，为下一步逆傅里叶变换做准备
        //}
        //transformBluestein(outR, outI);
        //for (int i = 0; i < outI.length; i++) {
        //outI[i]=(-outI[i])/outI.length;//取共轭，再除于变换序列的长度
        //outR[i]=outR[i]/outR.length;
        //}


        for (int i = 0; i < c2.length; i++) {
            System.out.println(c2[i]);
        }

        System.out.println("\n\n");

        for (int i = 0; i < outR.length; i++) {
            System.out.println(outR[i]+"  "+outI[i]);
        }
    }


}
