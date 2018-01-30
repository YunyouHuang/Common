package com.FileOperate;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.util.Date;

/*
 * 面向HDFS文件的操作
 * Created by hdp on 16-9-22.
 */
public class HDFSOperate implements Serializable{

    //支持只写路径，不写文件名:暂时用作边编号
    public void readFile(String inputPath) throws IOException {

        long index=0;
        long writCount=0;
        String tmpLine="";
        FileSystem fs = FileSystem.get(URI.create(inputPath),new Configuration());
        FileStatus[] fileList = fs.listStatus(new Path(inputPath));
        BufferedReader in = null;
        FSDataInputStream fsi = null;
        String line = null;
        for(int i = 0; i < fileList.length; i++){
            if(!fileList[i].isDirectory()){
                fsi = fs.open(fileList[i].getPath());
                in = new BufferedReader(new InputStreamReader(fsi,"UTF-8"));
                while((line = in.readLine()) != null){
                    System.out.println("read a line: num"+index+"  " + line);
                    tmpLine+=index+","+line+"\n";
                    index++;
                    writCount++;
                    if (writCount>=200000){
                        writCount=0;
                        FileWrite fw=new FileWrite();
                        fw.WriteTxt(tmpLine,"/home/hdp/LocalHDFSToDownAndLoad/test201609222208/markedData.txt");
                        tmpLine="";
                    }
                }
            }
        }
        FileWrite fw=new FileWrite();
        fw.WriteTxt(tmpLine,"/home/hdp/LocalHDFSToDownAndLoad/test201609222208/markedData.txt");
        in.close();
        fsi.close();
    }

    //hdfsPath指定存放在HDFS上的路径，不包括文件名称
    public static void putLocal2HDFS(String localFile,String hdfsPath) throws IOException{
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.support.append", true);
        File local=new File(localFile);
        if (local.exists()){
            FileInputStream in=new FileInputStream(local);
            String url=hdfsPath+local.getName().replace(".txt","_")+ InetAddress.getLocalHost().getHostName().replace(".","_")+"_"+new Date().getTime()+".txt";
            //String url=hdfsPath+local.getName();
            //url.replace(" ","_");
            FileSystem fs=FileSystem.get(URI.create(url),conf);
            OutputStream  out = fs.create(new Path(url));
            IOUtils.copyBytes(in,out,conf);
        }

    }

    public static void writeToHdfs(String pathStr,String context) throws IOException {
        Path path=new Path(pathStr);
        Configuration conf=new Configuration();
        FileSystem fs=null;
        fs=path.getFileSystem(conf);
        if (!fs.exists(path)){
            FSDataOutputStream outTmp=fs.create(path);
            outTmp.close();
        }
        FSDataOutputStream out=fs.append(path);
        out.write(context.getBytes("UTF-8"));
        out.flush();
        out.close();
       // fs.close();
    }
    public static void main(String[] args) throws IOException {

        writeToHdfs("hdfs://10.30.5.137:9000/spark/test/writeTest3.txt","454545454545453354\n");
        //HDFSOperate hp=new HDFSOperate();
        //hp.readFile("hdfs://10.30.5.137:9000/spark/test/SynthesisData/");
       // hp.putLocal2HDFS();
        //putLocal2HDFS("/home/hdp/LocalHDFSToDownAndLoad/test201609222208/markedData.txt","hdfs://10.30.5.137:9000/spark/test/test201609222143/markData/");
    }

}
