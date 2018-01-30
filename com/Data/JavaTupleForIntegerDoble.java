package com.Data;

import java.io.Serializable;

/**
 * Created by hdp on 16-10-12.
 */
public class JavaTupleForIntegerDoble implements Serializable {

    private int _1;
    private double _2;

    public int get_1() {
        return _1;
    }

    public void set_1(int _1) {
        this._1 = _1;
    }

    public double get_2() {
        return _2;
    }

    public void set_2(double _2) {
        this._2 = _2;
    }

    public JavaTupleForIntegerDoble(int x,double y){
        this._1=x;
        this._2=y;
    }
}
