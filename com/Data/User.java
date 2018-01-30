package com.Data;

/**
 * Created by hdp on 17-3-29.
 */
public class User {

    public long userID;
    public int age;
    public String address;
    public String industry;

    public long getID(){
        return this.userID;
    }

    public void setID(long id){
        this.userID=id;
    }
    public int getAge(){
        return this.age;
    }
    public void setEge(int age){
        this.age=age;
    }

    public String getAdr(){
        return this.address;
    }
    public void setAdr(String adr){
        this.address=adr;
    }
    public String getIndustry(){
        return this.industry;
    }
    public void setIndustry(String industry){
        this.industry=industry;
    }

    public User(){

    }

    public User(String data){
        String[] tmpStr=data.split(",");
        userID=Long.parseLong(tmpStr[0]);
        age=Integer.parseInt(tmpStr[1]);
        address=tmpStr[2];
        industry=tmpStr[3];
    }

    public String userToString(){
        return userID+","+age+","+address+","+industry;
    }
}
