package com.date;

import java.util.ArrayList;

/**
 * Created by hdp on 17-3-27.
 */
public class DateOperate {

    public static int getNewDate(String oldDate,int addDaysNum){
        int tmpYear=Integer.parseInt(oldDate.substring(0,4));
        int tmpMonth=Integer.parseInt(oldDate.substring(4,6));
        int tmpDay=Integer.parseInt(oldDate.substring(6,8))+addDaysNum;

        int secondMonthDays=28;

        if ((tmpYear%100)==0){
            if ((tmpYear%400)==0){
                secondMonthDays=29;
            }
        }else {
            if ((tmpYear%4)==0){
                secondMonthDays=29;
            }
        }

        if ((tmpDay>secondMonthDays)&&(tmpMonth==2)){
            tmpMonth=3;
            tmpDay=tmpDay-secondMonthDays;
        }else if ((tmpDay>30)
                &&((tmpMonth==4)
                ||(tmpMonth==6)
                ||(tmpMonth==9)
                ||(tmpMonth==11))){
            tmpMonth=tmpMonth+1;
            tmpDay=tmpDay-30;
        }else if ((tmpDay>31)
                &&((tmpMonth==1)
                ||(tmpMonth==3)
                ||(tmpMonth==5)
                ||(tmpMonth==7)
                ||(tmpMonth==8)
                ||(tmpMonth==10)
                ||(tmpMonth==12)
        )){
            if (tmpMonth==12){
                tmpMonth=1;
                tmpYear=tmpYear+1;
            }else {
                tmpMonth=tmpMonth+1;
            }
            tmpDay=tmpDay-31;
        }

        String dateStr="";
        dateStr+=tmpYear;
        if (tmpMonth<10){
            dateStr+="0"+tmpMonth;
        }else {
            dateStr+=tmpMonth;
        }
        if (tmpDay<10){
            dateStr+="0"+tmpDay;
        }else {
            dateStr+=tmpDay;
        }

        return Integer.parseInt(dateStr);
    }

    public static int getNewDate(String oldDate, int addDaysNum, ArrayList<Integer> delDate){
        int tmpYear=Integer.parseInt(oldDate.substring(0,4));
        int tmpMonth=Integer.parseInt(oldDate.substring(4,6));
        int tmpDay=Integer.parseInt(oldDate.substring(6,8))+addDaysNum;

        int secondMonthDays=28;

        if ((tmpYear%100)==0){
            if ((tmpYear%400)==0){
                secondMonthDays=29;
            }
        }else {
            if ((tmpYear%4)==0){
                secondMonthDays=29;
            }
        }

        if ((tmpDay>secondMonthDays)&&(tmpMonth==2)){
            tmpMonth=3;
            tmpDay=tmpDay-secondMonthDays;
        }else if ((tmpDay>30)
                &&((tmpMonth==4)
                ||(tmpMonth==6)
                ||(tmpMonth==9)
                ||(tmpMonth==11))){
            tmpMonth=tmpMonth+1;
            tmpDay=tmpDay-30;
        }else if ((tmpDay>31)
                &&((tmpMonth==1)
                ||(tmpMonth==3)
                ||(tmpMonth==5)
                ||(tmpMonth==7)
                ||(tmpMonth==8)
                ||(tmpMonth==10)
                ||(tmpMonth==12)
        )){
            if (tmpMonth==12){
                tmpMonth=1;
                tmpYear=tmpYear+1;
            }else {
                tmpMonth=tmpMonth+1;
            }
            tmpDay=tmpDay-31;
        }

        String dateStr="";
        dateStr+=tmpYear;
        if (tmpMonth<10){
            dateStr+="0"+tmpMonth;
        }else {
            dateStr+=tmpMonth;
        }
        if (tmpDay<10){
            dateStr+="0"+tmpDay;
        }else {
            dateStr+=tmpDay;
        }

         int tmpNewDate=Integer.parseInt(dateStr);

        for (int i = 0; i < delDate.size()/2; i++) {
            int sDay=delDate.get(2*i);
            int eDay=delDate.get(2*i+1);
            if ((eDay>=tmpNewDate)&&(sDay<=tmpNewDate)){
                tmpNewDate=getNewDate(String.valueOf(eDay),1);
            }
        }

        return tmpNewDate;
    }
}
