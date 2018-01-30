package com.Transform;

import JavaTuple.Tupleid;
import com.Similarity.Distance;

import java.text.DecimalFormat;
import java.util.ArrayList;

/**
 * Created by hdp on 17-6-22.
 */
public class FFT {

    // compute the FFT of x[], assuming its length is a power of 2
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
            y[k]       = q[k].plus(wk.times(r[k]));
            y[k + N/2] = q[k].minus(wk.times(r[k]));
        }
        return y;
    }


    // compute the inverse FFT of x[], assuming its length is a power of 2
    public static Complex[] ifft(Complex[] x) {
        int N = x.length;
        Complex[] y = new Complex[N];

        // take conjugate
        for (int i = 0; i < N; i++) {
            y[i] = x[i].conj();
        }

        // compute forward FFT
        y = fft(y);

        // take conjugate again
        for (int i = 0; i < N; i++) {
            y[i] = y[i].conj();
        }

        // divide by N
        for (int i = 0; i < N; i++) {
            y[i] = y[i].times(1.0 / N);
        }

        return y;

    }

    // compute the circular convolution of x and y
    public static Complex[] cconvolve(Complex[] x, Complex[] y) {

        // should probably pad x and y with 0s so that they have same length
        // and are powers of 2
        if (x.length != y.length) { throw new RuntimeException("Dimensions don't agree"); }

        int N = x.length;

        // compute FFT of each sequence
        Complex[] a = fft(x);
        Complex[] b = fft(y);

        // point-wise multiply
        Complex[] c = new Complex[N];
        for (int i = 0; i < N; i++) {
            c[i] = a[i].times(b[i]);
        }

        // compute inverse FFT
        return ifft(c);
    }


    // compute the linear convolution of x and y
    public static Complex[] convolve(Complex[] x, Complex[] y) {
        Complex ZERO = new Complex(0, 0);

        Complex[] a = new Complex[2*x.length];
        for (int i = 0;        i <   x.length; i++) a[i] = x[i];
        for (int i = x.length; i < 2*x.length; i++) a[i] = ZERO;

        Complex[] b = new Complex[2*y.length];
        for (int i = 0;        i <   y.length; i++) b[i] = y[i];
        for (int i = y.length; i < 2*y.length; i++) b[i] = ZERO;

        return cconvolve(a, b);
    }

    // display an array of Complex numbers to standard output
    public static void show(Complex[] x, String title) {
        DecimalFormat df = new DecimalFormat("#.####");
        System.out.println(title);
        System.out.println("-------------------");
        for (int i = 0; i < x.length; i++) {
            System.out.println(df.format(x[i].real())+"   "+df.format(x[i].imag())+"i");
            System.out.println();
        }
        System.out.println();
    }

    public static double getNorm2 (ArrayList<Double> x,ArrayList<Double> y) {
        Distance dist=new Distance();
        return Math.sqrt(dist.getPowDistance(x,new ArrayList<Double>()))*Math.sqrt(dist.getPowDistance(y,new ArrayList<Double>()));
    }

    public static int greater2p2(int n) {
        for (int i = 1;; i++) {
            if ((2*n-1) < Math.pow(2, i)) {
                return (int) Math.pow(2, i);
            }
        }
    }

    public static Complex[] transArray2Complex(ArrayList<Double> data){
        int comLenght=greater2p2(data.size());
        Complex[] retValue=new Complex[comLenght];
        for (int i = 0; i < comLenght; i++) {
            if (i<data.size()){
                retValue[i]=new Complex(data.get(i),0);
            }else {
                retValue[i]=new Complex(0,0);
            }
        }
        return retValue;
    }

    public static Complex[] NCCc(Complex[] x,Complex[] y,int dataLenght,double norm) {
        // should probably pad x and y with 0s so that they have same length
        // and are powers of 2
        if (x.length != y.length) { throw new RuntimeException("Dimensions don't agree"); }

        int N = x.length;

        // compute FFT of each sequence
        Complex[] a = fft(x);
        Complex[] b = fft(y);

        // point-wise multiply
        Complex[] c = new Complex[N];
        for (int i = 0; i < N; i++) {
            c[i] = a[i].times(b[i].conj());
        }

        // compute inverse FFT
        Complex[] d=ifft(c);
        int vexLenght=x.length;

        Complex[] retValue=new Complex[2*dataLenght-1];
        int tmpIndex=0;
        for (int i = vexLenght-dataLenght+1; i < vexLenght; i++) {
            retValue[tmpIndex]=new Complex(d[i].real()/norm, d[i].imag()/norm);
            tmpIndex++;
        }
        for (int i = 0; i < dataLenght; i++) {
            retValue[tmpIndex]=new Complex(d[i].real()/norm, d[i].imag()/norm);
            tmpIndex++;
        }
        return retValue;
    }

    public static Tupleid getSDB(ArrayList<Double> center,ArrayList<Double> y ){
        if (center.size()!=y.size()){
            System.out.println("size not compatible!");
            return new Tupleid(-1,1);
        }
        double norm=getNorm2(center,y);
        int dataSize=y.size();
        Complex[] ccov = NCCc(transArray2Complex(center), transArray2Complex(y),dataSize,norm);
        int bestIndex=-1;
        double _max=Double.MIN_VALUE;
        for (int i = 0; i < ccov.length; i++) {
            if (ccov[i].real()>_max){
                _max=ccov[i].real();
                bestIndex=i;
            }
        }
        return new Tupleid(bestIndex+1-y.size(),1-_max);
    }

    public static void main(String[] args) {
        //int N = Integer.parseInt(args[0]);
        double[] data={1,2,3,4,5,6};

        Complex[] x = new Complex[16];

        // original data
        for (int i = 0; i < 16; i++) {
            if (i<data.length) {
                x[i] = new Complex(data[i], 0);
            }else {
                x[i] = new Complex(0, 0);
            }
            //x[i] = new Complex(i, 0);

        }
        show(x, "x");

        // FFT of original data
        Complex[] y = fft(x);
        show(y, "y = fft(x)");

        // take inverse FFT
        Complex[] z = ifft(y);
        show(z, "z = ifft(y)");

        // circular convolution of x with itself
        Complex[] c = cconvolve(x, x);
        show(c, "c = cconvolve(x, x)");

        // linear convolution of x with itself
        Complex[] d = convolve(x, x);
        show(d, "d = convolve(x, x)");

        // linear convolution of x with itself
        Complex[] e = NCCc(x, x,6,Math.sqrt(91)*Math.sqrt(91));
        show(e, "e = NCCc(x, x)");
    }
}
