package com.Data;

import java.io.Serializable;

/**
 * Created by hdp on 16-9-16.
 */
public class Edge implements Serializable {

    private long starV=-1;
    private long endV=-1;
    private double weight=Double.MAX_VALUE;

    public long getEndV() {
        return endV;
    }

    public void setEndV(long endV) {
        this.endV = endV;
    }

    public long getStarV() {
        return starV;
    }

    public void setStarV(long starV) {
        this.starV = starV;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }
}
