package com.Data;

import scala.math.Ordered;

import java.io.Serializable;

/**
 * Created by hdp on 16-10-25.
 */
public class SortdKey implements Ordered<SortdKey>,Serializable {
    private static final long serialVersionUID = 1L;
    long dataID=-1;
    double dist=0;


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SortdKey that = (SortdKey) o;

        if (this.dataID != that.dataID) return false;
        return this.dist == that.dist;

    }

    @Override
    public int hashCode() {
        int result = (int) this.dataID;
        result = (int) (31 * result + dist);
        return result;
    }



    public SortdKey(long x,double y){
        this.dataID=x;
        this.dist=y;
    }

    @Override
    public int compare(SortdKey that) {
        if (this.$greater(that)){
            return 1;
        }else if (this.$less(that)){
           return  -1;
        }
        return 0;
    }

    @Override
    public boolean $less(SortdKey that) {
        if (this.dataID<that.dataID){
            return true;
        }else if ((this.dataID==that.dataID)&&(this.dist<that.dist)){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SortdKey that) {
        if (this.dataID>that.dataID){
            return true;
        }else if ((this.dataID==that.dataID)&&(this.dist>that.dist)){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SortdKey that) {
        if (this.$less(that)){
            return true;
        }else if ((this.dataID==that.dataID)&&(this.dist==that.dist)){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SortdKey that) {
        if (this.$greater(that)){
            return true;
        }else if ((this.dataID==that.dataID)&&(this.dist==that.dist)){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(SortdKey that) {
        if (this.$greater(that)){
            return 1;
        }
        else if (this.$less(that)){
            return -1;
        }
        return 0;
    }

    public long getFirst() {
        return dataID;
    }

    public void setFirst(long x) {
        this.dataID = x;
    }

    public double getSecond() {
        return dist;
    }

    public void setSecond(double y) {
        this.dist = y;
    }
}
