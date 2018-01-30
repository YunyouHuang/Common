package com.Chart;

import JavaTuple.Tuple3IFP;
import com.Data.ElectricProfile;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.awt.*;
import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;


/**
 * 用于画曲线
 * Created by hdp on 16-9-11.
 */
public class LineChart extends ApplicationFrame{

    public LineChart(String title) {
        super(title);
        // TODO Auto-generated constructor stub
    }
    //2D画曲线图，传入图标标题，X轴名称，y轴名称数据集，虚实线标记以及刻度显示标记
    public void drawLine2D(String title,String xLable,String yLable,DefaultCategoryDataset dataSet,int[] flag,boolean xy) {
        JFreeChart chart=ChartFactory.createLineChart(title, xLable, yLable, dataSet,PlotOrientation.VERTICAL,true,true,false);
        Font titleFont=new Font("宋体", Font.BOLD, 22);
        Font font=new Font("宋体", Font.PLAIN, 15);
        chart.getTitle().setFont(titleFont); // 设置标题字体
        chart.getLegend().setItemFont(font);// 设置图例类别字体
        // chart.setBackgroundPaint(bgColor);// 设置背景色
        //获取绘图区对象
        CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(Color.LIGHT_GRAY); // 设置绘图区背景色
        plot.setRangeGridlinePaint(Color.WHITE); // 设置水平方向背景线颜色
        plot.setRangeGridlinesVisible(true);// 设置是否显示水平方向背景线,默认值为true
        plot.setDomainGridlinePaint(Color.WHITE); // 设置垂直方向背景线颜色
        plot.setDomainGridlinesVisible(true); // 设置是否显示垂直方向背景线,默认值为false


        CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setLabelFont(font); // 设置横轴字体
        //  domainAxis.setTick
        domainAxis.setTickLabelFont(font);// 设置坐标轴标尺值字体
        domainAxis.setLowerMargin(0.01);// 左边距 边框距离
        domainAxis.setUpperMargin(0.06);// 右边距 边框距离,防止最后边的一个数据靠近了坐标轴。
        domainAxis.setMaximumCategoryLabelLines(2);

        ValueAxis rangeAxis = plot.getRangeAxis();
        rangeAxis.setLabelFont(font);
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());//Y轴显示整数
        rangeAxis.setAutoRangeMinimumSize(1);   //最小跨度
        rangeAxis.setUpperMargin(0.18);//上边距,防止最大的一个数据靠近了坐标轴。
        rangeAxis.setLowerBound(0);   //最小值显示0
        rangeAxis.setAutoRange(false);   //不自动分配Y轴数据
        rangeAxis.setTickMarkStroke(new BasicStroke(1.6f));     // 设置坐标标记大小
        rangeAxis.setTickMarkPaint(Color.BLACK);     // 设置坐标标记颜色

        // 获取折线对象
        LineAndShapeRenderer renderer = (LineAndShapeRenderer) plot.getRenderer();
        BasicStroke realLine = new BasicStroke(1.8f); // 设置实线
        // 设置虚线
        float dashes[] = { 5.0f };
        BasicStroke brokenLine = new BasicStroke(2.2f, // 线条粗细
                BasicStroke.CAP_ROUND, // 端点风格
                BasicStroke.JOIN_ROUND, // 折点风格
                8f, dashes, 0.6f);
        for (int i = 0; i < dataSet.getRowCount(); i++) {
            if (flag[i]==1)
                renderer.setSeriesStroke(i, realLine); // 利用实线绘制
            else
                renderer.setSeriesStroke(i, brokenLine); // 利用虚线绘制
        }

        plot.setNoDataMessage("无对应的数据，请重新查询。");
        plot.setNoDataMessageFont(titleFont);//字体的大小
        plot.setNoDataMessagePaint(Color.RED);//字体颜色

        ChartPanel chartPanel = new ChartPanel( chart );
        chartPanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
        setContentPane( chartPanel );

    }



    //2D画曲线图，传入图标标题，X轴名称，y轴名称数据集，虚实线标记以及刻度显示标记
    public void drawLine2DXY(String title, String xLable, String yLable, XYDataset dataSet, ArrayList<Tuple3IFP> style,int labelFlag) {

        JFreeChart chart =ChartFactory.createXYLineChart(title, xLable, yLable, dataSet, PlotOrientation.VERTICAL, true, true, false);
        Font titleFont=new Font("宋体", Font.BOLD, 22);
        //Font font=new Font("宋体", Font.PLAIN, 22);

        Font font=new Font("宋体", Font.BOLD, 22);
        chart.getTitle().setFont(titleFont); // 设置标题字体
        if (labelFlag==0){
            chart.getLegend().setVisible(false);
        }else if (labelFlag==1){
            chart.getLegend().setItemFont(font);// 设置图例类别字体
        }else {
            if (style.size()<=10){
                chart.getLegend().setItemFont(font);// 设置图例类别字体
            }else{
                chart.getLegend().setVisible(false);
            }
        }

       // if (style.size()<=10){
            //chart.getLegend().setItemFont(font);// 设置图例类别字体
            //chart.getLegend().setVisible(false);
        //}else{
            //chart.getLegend().setVisible(false);
        //}

        chart.setBackgroundPaint(Color.white);// 设置背景色
        //获取绘图区对象
        XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.WHITE); // 设置绘图区背景色
        plot.setRangeGridlinePaint(Color.WHITE); // 设置水平方向背景线颜色
        plot.setRangeGridlinesVisible(true);// 设置是否显示水平方向背景线,默认值为true
        plot.setDomainGridlinePaint(Color.WHITE); // 设置垂直方向背景线颜色
        plot.setDomainGridlinesVisible(true); // 设置是否显示垂直方向背景线,默认值为false


        ValueAxis domainAxis = plot.getDomainAxis();
        domainAxis.setLabelFont(font); // 设置横轴字体
        domainAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        domainAxis.setTickLabelFont(font);// 设置坐标轴标尺值字体
        domainAxis.setLowerMargin(0.01);// 左边距 边框距离
        domainAxis.setUpperMargin(0.1);// 右边距 边框距离,防止最后边的一个数据靠近了坐标轴。
        //  domainAxis.setMaximumCategoryLabelLines(2);
        domainAxis.setAutoRangeMinimumSize(1);
        domainAxis.setAutoRange(false);
        domainAxis.setTickMarkStroke(new BasicStroke(1.6f));     // 设置坐标标记大小
        domainAxis.setTickMarkPaint(Color.BLACK);     // 设置坐标标记颜色
        domainAxis.setPositiveArrowVisible(true);


        ValueAxis rangeAxis = plot.getRangeAxis();
        rangeAxis.setLabelFont(font);
        //rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());//Y轴显示整数
        rangeAxis.setTickLabelFont(font);
        rangeAxis.setAutoRangeMinimumSize(1);   //最小跨度
        rangeAxis.setUpperMargin(0.18);//上边距,防止最大的一个数据靠近了坐标轴。
        rangeAxis.setLowerBound(0);   //最小值显示0
        rangeAxis.setAutoRange(false);   //不自动分配Y轴数据
        rangeAxis.setTickMarkStroke(new BasicStroke(1.6f));     // 设置坐标标记大小
        rangeAxis.setTickMarkPaint(Color.BLACK);     // 设置坐标标记颜色
        rangeAxis.setPositiveArrowVisible(true);

        // 获取折线对象
        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
        for (int i = 0; i < dataSet.getSeriesCount(); i++) {
            Tuple3IFP tmpStyle=style.get(i);
            BasicStroke line=new BasicStroke();
            if (tmpStyle._1()==1){
                line=new BasicStroke(tmpStyle._2());
            }else {
                float dashes[] = { tmpStyle._2() };
                line=new BasicStroke(tmpStyle._2(),BasicStroke.CAP_ROUND,BasicStroke.JOIN_ROUND,8f, dashes, 0.6f);
            }
            if (tmpStyle._3()!=null) {
                renderer.setSeriesPaint(i, tmpStyle._3());
            }
            renderer.setSeriesStroke(i, line); // 利用实线绘制

            if (((style.size()==1)||(style.size()>10))&&(tmpStyle._3()==null)){
                renderer.setSeriesPaint(i,Color.BLACK);
            }

            //if ((style.size()==1)&&(tmpStyle._3()==null)){
                //renderer.setSeriesPaint(i,Color.BLACK);
            //}
            //renderer.setSeriesPaint(i,Color.BLACK);
            //renderer.setSeriesPaint(i,Color.GREEN);
        }

        plot.setNoDataMessage("无对应的数据，请重新查询。");
        plot.setNoDataMessageFont(titleFont);//字体的大小
        plot.setNoDataMessagePaint(Color.RED);//字体颜色

        ChartPanel chartPanel = new ChartPanel( chart );
        chartPanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
        setContentPane( chartPanel );

    }


    public XYDataset getXYDataSet(String path,String clu){

        XYSeriesCollection sc=new XYSeriesCollection();
        //dint count=0;
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                String[] tmpStr=line.substring(1,line.length()-1).split(",",2);
                ElectricProfile tmpEle=new ElectricProfile(tmpStr[1]);

                //System.out.println(tmpStr[0]);
                if (tmpStr[0].trim().equals(clu)) {
                    System.out.println(tmpStr[0]);
                    XYSeries s1 = new XYSeries(tmpEle.getID() + "-" + tmpEle.getDataDate());
                    for (int i = 0; i < tmpEle.getPoints().size(); i++) {
                        s1.add(i, tmpEle.getPoints().get(i));
                    }
                    sc.addSeries(s1);
                }
               // count++;
                //if (count>2200){
                   // break;
                //}
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sc;
    }

    public XYDataset getXYDataSetMul(String path,String clu,int number,double rate){
        int count=0;
        XYSeriesCollection sc=new XYSeriesCollection();
        File file = new File(path);
        if (file.isDirectory()) {
            String[] filelist = file.list();
            for (int i = 0; i < filelist.length; i++) {
                File readfile = new File(path + "/" + filelist[i]);
                if (!readfile.isDirectory()) {
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new InputStreamReader(new FileInputStream(readfile), "UTF-8"));
                        String line = null;
                        while ((line = reader.readLine()) != null) {
                            String[] tmpStr=line.substring(1,line.length()-1).split(",",2);
                            ElectricProfile tmpEle=new ElectricProfile(tmpStr[1]);
                            //System.out.println(tmpStr[0]);

                            if (tmpStr[0].trim().equals(clu)) {
                                if (rate<1){
                                    if (Math.random()<=rate){
                                        System.out.println(tmpStr[0]);
                                        XYSeries s1 = new XYSeries(tmpEle.getID() + "-" + tmpEle.getDataDate()+"-"+count);
                                        for (int j = 0; j < tmpEle.getPoints().size(); j++) {
                                            s1.add(j, tmpEle.getPoints().get(j));
                                        }
                                        sc.addSeries(s1);
                                        count++;
                                        if ((number!=-1)&&(count>=number)){
                                            break;
                                        }
                                    }
                                }else {
                                    System.out.println(tmpStr[0]);
                                    XYSeries s1 = new XYSeries(tmpEle.getID() + "-" + tmpEle.getDataDate()+"-"+count);
                                    for (int j = 0; j < tmpEle.getPoints().size(); j++) {
                                        s1.add(j, tmpEle.getPoints().get(j));
                                    }
                                    sc.addSeries(s1);
                                    count++;
                                    if ((number!=-1)&&(count>=number)){
                                        break;
                                    }
                                }
                            }
                        }
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                if ((number!=-1)&&(count>=number)){
                    break;
                }
            }
        }
        return sc;
    }

    public XYDataset getXYDataSet2(String path){

        XYSeriesCollection sc=new XYSeriesCollection();
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                ElectricProfile tmpEle=new ElectricProfile(line);
                XYSeries s1=new XYSeries(tmpEle.getID()+"-"+tmpEle.getDataDate());
                for (int i = 0; i < tmpEle.getPoints().size(); i++) {
                    s1.add(i,tmpEle.getPoints().get(i));
                }
                sc.addSeries(s1);
            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sc;
    }


    public XYDataset getXYDataSet3(String path,String num){

        XYSeriesCollection sc=new XYSeriesCollection();
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                String[] tmpStr=line.split(",");
                String[] numAry=num.split(",");
                HashSet<String> key=new HashSet<String>();
                for (int i = 0; i < numAry.length; i++) {
                    key.add(numAry[i]);
                }
                if (key.contains(tmpStr[0])){
                    XYSeries s1=new XYSeries(tmpStr[0]);
                    for (int i = 1; i < tmpStr.length; i++) {
                        s1.add(i,Double.parseDouble(tmpStr[i]));
                    }
                    sc.addSeries(s1);
                }

            }

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sc;
    }


    public XYDataset getXYDataSetEva(String path,String num){

        XYSeriesCollection sc=new XYSeriesCollection();
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
            String line=null;
            XYSeries k_0_50_DB=new XYSeries("BD");
            XYSeries k_0_50_S_Dbw=new XYSeries("S_Dbw-0");
            XYSeries k_2_50_S_DB=new XYSeries("BD");
            XYSeries k_2_50_S_Dbw=new XYSeries("S_Dbw-2");

            XYSeries K_2_DB=new XYSeries("BD");
            XYSeries K_2_S_Dbw=new XYSeries("S_Dbw");

            while ((line=reader.readLine())!=null){
                String[] tmpStr=line.split(",");
                String name=tmpStr[0];
                String claculateFalg=tmpStr[1];
                int clusterNum=Integer.parseInt(tmpStr[2]);
                double vioRate=Double.parseDouble(tmpStr[3]);
                double dbi=Double.parseDouble(tmpStr[5]);
                double sdbw=Double.parseDouble(tmpStr[4]);
                if (clusterNum<51){
                    if (claculateFalg.equals("0")){
                        k_0_50_DB.add(clusterNum,dbi);
                        k_0_50_S_Dbw.add(clusterNum,sdbw);
                    }else if (claculateFalg.equals("2")){
                        k_2_50_S_DB.add(clusterNum,dbi);
                        k_2_50_S_Dbw.add(clusterNum,sdbw);
                    }
                }

                if (claculateFalg.equals("2")){
                    K_2_DB.add(clusterNum,dbi);
                    K_2_S_Dbw.add(clusterNum,sdbw);
                }
            }

            //sc.addSeries(k_0_50_DB);
            //sc.addSeries(k_0_50_S_Dbw);
            //sc.addSeries(k_2_50_S_DB);
            //sc.addSeries(k_2_50_S_Dbw);
            //sc.addSeries(K_2_DB);
            sc.addSeries(K_2_S_Dbw);

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sc;
    }



    public static void main(String[] args) {
        // TODO Auto-generated method stub

        //region
        /*
        DefaultCategoryDataset dataset=new DefaultCategoryDataset();
        //dataset.addValue(5, );
        dataset.addValue(1, "workers", "1");
        dataset.addValue(2, "workers", "2");
        dataset.addValue(3, "workers", "3");
        dataset.addValue(8, "workers", "4");
        dataset.addValue(7, "workers", "5");


        dataset.addValue(10, "farmers", "1");
        dataset.addValue(20, "farmers", "2");
        dataset.addValue(18, "farmers", "3");
        dataset.addValue(50, "farmers", "4");
        dataset.addValue(33, "farmers", "5");


        XYSeries s1=new XYSeries("");
        s1.add(1,1);
        s1.add(2,3);
        s1.add(3,2);
        s1.add(4,8);
        s1.add(5,5);

        XYSeries s2=new XYSeries("ggg");
        s2.add(1,7);
        s2.add(3,1);
        s2.add(5,6);
        s2.add(2,8);
        s2.add(4,9);
        s2.add(7,7);

        XYSeriesCollection sc=new XYSeriesCollection();
        sc.addSeries(s1);
        sc.addSeries(s2);


        ArrayList<Tuple3<Integer,Float,Paint>> style=new ArrayList<Tuple3<Integer, Float, Paint>>();
        style.add(new Tuple3<Integer, Float, Paint>(1,2.2f,null));
        //style.add(new Tuple3<Integer, Float, Paint>(1,2.2f,null));
        //int[] flag={1,0};
        */
        //endregion



        //String title=args[0];
        //String xlable=args[1];
        //String ylable=args[2];
        //String dataPath=args[3];

        //String cluNum="Clustering Validity Assessment";
        //LineChart lChart=new LineChart("Clustering Validity Assessment");
        //XYDataset dataSet=lChart.getXYDataSetEva("/home/hdp/eleProfile/EvaAll.txt",cluNum);

        String cNum=args[0];
        String dataPath=args[1];
        int num=Integer.parseInt(args[2]);
        double rate=Double.parseDouble(args[3]);
        float fontSize=Float.parseFloat(args[4]);
        int drawFlag=Integer.parseInt(args[5]);
        int labelFlag=Integer.parseInt(args[6]);
        int blackFlag=Integer.parseInt(args[7]);
        //String cluNum="194";
        LineChart lChart=new LineChart("Electricity consumption");
        //XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/K-KMeans-adSum-2-03-25000",cluNum,-1);

        //XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/20170519/geFor1950Classic",cluNum,-1);
        //XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/20170519/geFor1950Mul3",cluNum,-1);
        //XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/20170519/geForClassic-70yc",cluNum,-1);

        //XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/20170519/geWeek333",cluNum,-1);
        ////XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/20170519/KMeanes-W/K-KMeans-adSum-2-03-3000",cluNum,-1);
        //XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/20170519/generationForY7013647",cluNum,-1);
        //XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/20170519/KMeanes-Y/K-KMeans-adSum-2-03-80",cluNum,-1);
        //XYDataset dataSet=lChart.getXYDataSetMul("/home/hdp/eleProfile/0624/Result",cluNum,-1);
        XYDataset dataSet;
        if (drawFlag==0){
            dataSet=lChart.getXYDataSetMul(dataPath,cNum,num,rate);
        }else {
            dataSet=lChart.getXYDataSet3(dataPath,cNum);
        }

        //XYDataset dataSet=lChart.getXYDataSet3("/home/hdp/eleProfile/paper/2055-2.txt",cluNum);
        //XYDataset dataSet=lChart.getXYDataSet3("/home/hdp/eleProfile/270883.csv",cluNum);
        //XYDataset dataSet=lChart.getXYDataSet3("/home/hdp/eleProfile/K-KMeans-adSum-2-03-25000centers/centers.txt",cluNum);
        ArrayList<Tuple3IFP> style=new ArrayList<Tuple3IFP>();
        for (int i = 0; i < dataSet.getSeriesCount(); i++) {
            if (blackFlag==0){
                style.add(new Tuple3IFP(1,fontSize,null));
            }else {
                style.add(new Tuple3IFP(1,fontSize,Color.BLACK));
            }

        }

       // lChart.drawLine2D("测试", "xxxx", "people", dataset, flag, true);
        //lChart.drawLine2DXY(cluNum,"No clusters","Assessment value",dataSet,style);
        lChart.drawLine2DXY(cNum,"Time/Quarter","Consumption/KWh",dataSet,style,labelFlag);
        lChart.pack( );
        RefineryUtilities.centerFrameOnScreen( lChart );
        lChart.setVisible( true );

    }


}
