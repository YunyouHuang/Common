package com.Sort;

import com.Data.ElectricProfile;
import com.Data.WeekAndYearInstance;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * Created by hdp on 17-4-21.
 */
public class InsertSort {

    public static ArrayList<ElectricProfile> sorForEleAscend(Iterable<ElectricProfile> data){
        ArrayList<ElectricProfile> retValue=new ArrayList<ElectricProfile>();
        for (ElectricProfile ele:data
             ) {
            retValue.add(ele);
        }

        int j;
        int n=retValue.size();
        ElectricProfile target;

        for (int k = 1; k <n ; k++) {
            j=k;
            target=retValue.get(k);
            while ((j>0)&&(Integer.parseInt(target.getDataDate())<Integer.parseInt(retValue.get(j-1).getDataDate()))){
                retValue.set(j,retValue.get(j-1));
                j--;
            }

            retValue.set(j,target);
        }
        return retValue;
    }

    public static ArrayList<Tuple2<Integer,ElectricProfile>> sortForEleWithMarkAscend(Iterable<Tuple2<Integer,ElectricProfile>> data){
        ArrayList<Tuple2<Integer,ElectricProfile>> retValue=new ArrayList<Tuple2<Integer,ElectricProfile>>();
        for (Tuple2<Integer,ElectricProfile> ele:data
                ) {
            retValue.add(ele);
        }

        int j;
        int n=retValue.size();
        Tuple2<Integer,ElectricProfile> target;

        for (int k = 1; k <n ; k++) {
            j=k;
            target=retValue.get(k);
            while ((j>0)&&(Integer.parseInt(target._2.getDataDate())<Integer.parseInt(retValue.get(j-1)._2.getDataDate()))){
                retValue.set(j,retValue.get(j-1));
                j--;
            }

            retValue.set(j,target);
        }
        return retValue;
    }



    public static ArrayList<WeekAndYearInstance> sorForWYAscend(Iterable<WeekAndYearInstance> data){
        ArrayList<WeekAndYearInstance> retValue=new ArrayList<WeekAndYearInstance>();
        for (WeekAndYearInstance ele:data
                ) {
            retValue.add(ele);
        }

        int j;
        int n=retValue.size();
        WeekAndYearInstance target;

        for (int k = 1; k <n ; k++) {
            j=k;
            target=retValue.get(k);
            while ((j>0)&&(Integer.parseInt(target.getDate())<Integer.parseInt(retValue.get(j-1).getDate()))){
                retValue.set(j,retValue.get(j-1));
                j--;
            }

            retValue.set(j,target);
        }
        return retValue;
    }

    public static ArrayList<Tuple2<Integer,WeekAndYearInstance>> sortForWYWithMarkAscend(Iterable<Tuple2<Integer,WeekAndYearInstance>> data){
        ArrayList<Tuple2<Integer,WeekAndYearInstance>> retValue=new ArrayList<Tuple2<Integer,WeekAndYearInstance>>();
        for (Tuple2<Integer,WeekAndYearInstance> ele:data
                ) {
            retValue.add(ele);
        }

        int j;
        int n=retValue.size();
        Tuple2<Integer,WeekAndYearInstance> target;

        for (int k = 1; k <n ; k++) {
            j=k;
            target=retValue.get(k);
            while ((j>0)&&(Integer.parseInt(target._2.getDate())<Integer.parseInt(retValue.get(j-1)._2.getDate()))){
                retValue.set(j,retValue.get(j-1));
                j--;
            }

            retValue.set(j,target);
        }
        return retValue;
    }


}
