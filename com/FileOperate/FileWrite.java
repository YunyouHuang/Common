package com.FileOperate;

import java.io.*;
import java.util.ArrayList;

/**
 * 本地读文件操作
 * Created by hdp on 16-9-19.
 */
public class FileWrite implements Serializable{
    /**
     * <summary>写文件操作</summary>
     * <param name="Text">写入的字符串</param>
     * <param name="Spath">写入的文件路径</param>
     */
    public static void WriteTxt(String Text,String Spath)
    {
        if (Spath.equals(""))
        {
            try {
                FileWriter writer = new FileWriter("./default/Result.txt", true);
                writer.write(Text);
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        else
        {
            try {
                File file=new File(Spath);
                if (!file.exists()){
                    if (!file.getParentFile().exists()){
                        file.getParentFile().mkdirs();
                    }
                    file.createNewFile();
                }
                FileWriter writer = new FileWriter(Spath, true);
                writer.write(Text);
                writer.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    public void deleteFile(String Spath){
        File file=new File(Spath);
        if (file.exists()){
            file.delete();
        }
    }

    public void readText(String Spath){
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(Spath),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){

            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }
    }

    public void readText2V(String filepath){
        File file = new File(filepath);
        if (!file.isDirectory()) {
            System.out.println("文件");
            System.out.println("path=" + file.getPath());
            System.out.println("absolutepath=" + file.getAbsolutePath());
            System.out.println("name=" + file.getName());
        } else if (file.isDirectory()) {
            System.out.println("文件夹");
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                File readfile = new File(filepath + "\\" + filelist[i]);
                if (!readfile.isDirectory()) {
                    BufferedReader reader=null;
                    try {
                        reader=new BufferedReader(new InputStreamReader(new FileInputStream(readfile),"UTF-8"));
                        String line=null;
                        while ((line=reader.readLine())!=null){

                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }finally {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException e1) {
                            }
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args){

        /*
        for (int i = 0; i <100000 ; i++) {
            long ID= new Random().nextInt(8000000)+1;
            Date dataDate=new Date(new java.util.Date().getTime());
            int dataType=1;
            String orgNO="kjhhjkh";
            Date getDate=new Date(new java.util.Date().getTime());
            int dataPointFlag=96;
            String dataWholeFlag="1111111111111111111111111111111";
            ArrayList<Double> points=new ArrayList<Double>();

            for (int j = 0; j < 96; j++) {
                points.add(Math.random()*50);
            }

            String retValue="";
            retValue="("+i+","+ID+","+dataDate+","+dataType+","+orgNO+","+getDate+","+dataPointFlag+","+dataWholeFlag;
            for (int j = 0; j < points.size(); j++) {
                retValue+=","+points.get(j);
            }
            retValue+=")\n";
            FileWrite fw=new FileWrite();
            fw.WriteTxt(retValue,"/home/hdp/eleProfile/eleProMarked100000.txt");
        }
        */

        for (int i = 1000000; i < 1030000; i++) {
            long ID= (long)i;
            String dateStr="2015";
            int dataType=1;
            String orgNO=String.valueOf((int)(Math.random()*5+85624921));
            String getDate="20170213";
            int dataPointFlag=96;
            String dataWholeFlag="1111111111111111111111111111111";

            for (int j = 1; j <=12 ; j++) {

                if (j==2){
                    String mouth="02";
                    for (int k = 1; k < 29; k++) {
                        int del=(int)(Math.random()*29);
                        if (!((k>=(del-1))&&(k<=del+1))) {
                            String day="";
                            if (k<10){
                                day+="0"+k;
                            }else {
                                day+=k;
                            }

                            ArrayList<Double> points=new ArrayList<Double>();
                            for (int l = 0; l < 96; l++) {
                                points.add(Math.random()*50);
                            }

                            String retValue="";
                            retValue=ID+","+dateStr+mouth+day+","+dataType+","+orgNO+","+getDate+","+dataPointFlag+","+dataWholeFlag;
                            for (int l = 0; l < points.size(); l++) {
                                retValue+=","+points.get(l);
                            }
                            retValue+="\n";
                            FileWrite fw=new FileWrite();
                            fw.WriteTxt(retValue,"/home/hdp/eleProfile/3ele.txt");
                        }
                    }
                }else if((j==1)
                        ||(j==3)
                        ||(j==5)
                        ||(j==7)
                        ||(j==8)
                        ||(j==10)
                        ||(j==12)){
                    String mouth="";
                    if (j<10){
                        mouth+="0"+j;
                    }else {
                        mouth+=j;
                    }
                    for (int k = 1; k < 32; k++) {

                        int del=(int)(Math.random()*32);
                        if (!((k>=(del-1))&&(k<=del+1))) {
                            String day="";
                            if (k<10){
                                day+="0"+k;
                            }else {
                                day+=k;
                            }

                            ArrayList<Double> points=new ArrayList<Double>();
                            for (int l = 0; l < 96; l++) {
                                points.add(Math.random()*50);
                            }

                            String retValue="";
                            retValue=ID+","+dateStr+mouth+day+","+dataType+","+orgNO+","+getDate+","+dataPointFlag+","+dataWholeFlag;
                            for (int l = 0; l < points.size(); l++) {
                                retValue+=","+points.get(l);
                            }
                            retValue+="\n";
                            FileWrite fw=new FileWrite();
                            fw.WriteTxt(retValue,"/home/hdp/eleProfile/3ele.txt");
                        }
                    }

                }else {
                    String mouth="";
                    if (j<10){
                        mouth+="0"+j;
                    }else {
                        mouth+=j;
                    }
                    for (int k = 1; k < 31; k++) {
                        int del=(int)(Math.random()*31);
                        if (!((k>=(del-1))&&(k<=del+1))) {
                            String day="";
                            if (k<10){
                                day+="0"+k;
                            }else {
                                day+=k;
                            }

                            ArrayList<Double> points=new ArrayList<Double>();
                            for (int l = 0; l < 96; l++) {
                                points.add(Math.random()*50);
                            }

                            String retValue="";
                            retValue=ID+","+dateStr+mouth+day+","+dataType+","+orgNO+","+getDate+","+dataPointFlag+","+dataWholeFlag;
                            for (int l = 0; l < points.size(); l++) {
                                retValue+=","+points.get(l);
                            }
                            retValue+="\n";
                            FileWrite fw=new FileWrite();
                            fw.WriteTxt(retValue,"/home/hdp/eleProfile/3ele.txt");
                        }
                    }
                }
            }

        }


        /*
        for (int i = 1000000; i < 1000400; i++) {
            long ID= (long)i;
            //int inStage=(int)(Math.random()*300);
            //System.out.print(inStage+"\n");
            //String industry="industry"+inStage;
            //int stage=(int)(Math.random()*30);
           // String age=(0+stage*5)+"-"+(5+stage*5);
            String date="2015";
            //String addr=003+"-"+(int)(Math.random()*30)+"-"+(int)(Math.random()*9);
            String retValue="";

            for (int j = 0; j < 52; j++) {
                retValue=ID+","+date;
                for (int k = 0; k < 7; k++) {
                    retValue+=","+(int)(Math.random()*34);
                }
                retValue+="\n";
                FileWrite fw=new FileWrite();
                fw.WriteTxt(retValue,"/home/hdp/eleProfile/weekData.txt");
            }
        }
        */
    }
}
