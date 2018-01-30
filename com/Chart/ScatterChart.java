package com.Chart;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.awt.*;
import java.io.*;
import java.util.ArrayList;

/**
 * 散点图
 * Created by hdp on 16-9-11.
 */
public class ScatterChart extends ApplicationFrame{
    public ScatterChart(String title) {
        super(title);
    }

    public void drawScatterChartXY(String title, String xLable, String yLable, XYDataset dataSet, ArrayList<Paint> style){
        JFreeChart jFreeChart= ChartFactory.createScatterPlot(title,xLable,yLable,dataSet, PlotOrientation.VERTICAL,true,false,false);
        Font titleFont=new Font("宋体", Font.BOLD, 22);
        //Font font=new Font("宋体", Font.PLAIN, 15);

        Font font=new Font("宋体", Font.BOLD, 22);
        jFreeChart.getTitle().setFont(titleFont); // 设置标题字体
        jFreeChart.setBackgroundPaint(Color.white);
        jFreeChart.setBorderPaint(Color.GREEN);
        jFreeChart.setBorderStroke(new BasicStroke(1.5f));
        if (style.size()<=20){
            jFreeChart.getLegend().setItemFont(font);// 设置图例类别字体
            jFreeChart.getLegend().setVisible(false);
        }else{
            jFreeChart.getLegend().setVisible(false);
        }

        XYPlot plot = (XYPlot) jFreeChart.getPlot();
        plot.setNoDataMessage("无对应的数据，请重新查询。");
        plot.setNoDataMessageFont(new Font("", Font.BOLD, 14));
        plot.setNoDataMessagePaint(new Color(87, 149, 117));

        plot.setBackgroundPaint(Color.LIGHT_GRAY); // 设置绘图区背景色
        plot.setRangeGridlinePaint(Color.WHITE); // 设置水平方向背景线颜色
        plot.setRangeGridlinesVisible(true);// 设置是否显示水平方向背景线,默认值为true
        plot.setDomainGridlinePaint(Color.WHITE); // 设置垂直方向背景线颜色
        plot.setDomainGridlinesVisible(true); // 设置是否显示垂直方向背景线,默认值为false


        ValueAxis domainAxis = plot.getDomainAxis();
        domainAxis.setLabelFont(font); // 设置横轴字体
        domainAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        domainAxis.setTickLabelFont(font);// 设置坐标轴标尺值字体
        domainAxis.setLowerMargin(0.01);// 左边距 边框距离
        domainAxis.setUpperMargin(0.06);// 右边距 边框距离,防止最后边的一个数据靠近了坐标轴。
        //  domainAxis.setMaximumCategoryLabelLines(2);
        domainAxis.setAutoRangeMinimumSize(1);
        domainAxis.setAutoRange(false);
        domainAxis.setTickMarkStroke(new BasicStroke(1.6f));     // 设置坐标标记大小
        domainAxis.setTickMarkPaint(Color.BLACK);     // 设置坐标标记颜色
        domainAxis.setPositiveArrowVisible(true);


        ValueAxis rangeAxis = plot.getRangeAxis();
        rangeAxis.setLabelFont(font);
        rangeAxis.setLabelAngle(90);
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());//Y轴显示整数
        rangeAxis.setTickLabelFont(font);
        rangeAxis.setAutoRangeMinimumSize(1);   //最小跨度
        rangeAxis.setUpperMargin(0.18);//上边距,防止最大的一个数据靠近了坐标轴。
        rangeAxis.setLowerBound(0);   //最小值显示0
        rangeAxis.setAutoRange(false);   //不自动分配Y轴数据
        rangeAxis.setTickMarkStroke(new BasicStroke(1.6f));     // 设置坐标标记大小
        rangeAxis.setTickMarkPaint(Color.BLACK);     // 设置坐标标记颜色
        rangeAxis.setPositiveArrowVisible(true);

        XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) plot.getRenderer();
        //ScatterRenderer sRenderer=(ScatterRenderer) plot.getRenderer();
        for (int i = 0; i < dataSet.getSeriesCount(); i++) {
            //sRenderer.setSeriesShape(i,getShape());
            if (style.get(i)!=null){
                //renderer.setSeriesOutlinePaint(i, style.get(i));
                renderer.setSeriesPaint(i,style.get(i));
            }
            else{
                renderer.setSeriesPaint(i,Color.black);
            }

        }
        //sRenderer.setSer
        ChartPanel chartPanel = new ChartPanel( jFreeChart );
        chartPanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
        setContentPane( chartPanel );

    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        ///home/hdp/eleProfile
        //String title =args[0];
        //String xLable=args[1];
        //String yLable=args[2];
        //String dataSet=args[3];
        //String inputPath=args[4];

        String title ="Decision Graph";
        String xLable="ρ";
        String yLable="δ";
        String dataSet="pd";
        String inputPath="/home/hdp/eleProfile/20170519/desityAndDist/part-00000";

        XYSeries xyseries = new XYSeries(dataSet);
        BufferedReader reader=null;
        try {
            reader=new BufferedReader(new InputStreamReader(new FileInputStream(inputPath),"UTF-8"));
            String line=null;
            while ((line=reader.readLine())!=null){
                String tmpStr=line.substring(1,line.length()-1).split(",",2)[1];
                String[] dataDensityAndDist=tmpStr.substring(1,tmpStr.length()-1).split(",");
                xyseries.add(Double.parseDouble(dataDensityAndDist[0]),Double.parseDouble(dataDensityAndDist[1]));
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        /*
        xyseries.add(3.25,566);
        xyseries.add(6.25,523);
        xyseries.add(2.25,456);
        xyseries.add(0.25,326);
        xyseries.add(4.25,156);


        XYSeries xyseries2 = new XYSeries("lllll");
        xyseries2.add(1,536);
        xyseries2.add(3,500);
        xyseries2.add(2.25,400);
        xyseries2.add(10,300);
        xyseries2.add(8,106);
        */
        XYSeriesCollection xyseriescollection = new XYSeriesCollection(); //再用XYSeriesCollection添加入XYSeries 对象
        xyseriescollection.addSeries(xyseries);
        //xyseriescollection.addSeries(xyseries2);

        ArrayList<Paint> col=new ArrayList<Paint>();
        //col.add(Color.red);
        col.add(Color.black);

        ScatterChart sc=new ScatterChart(title);
        sc.drawScatterChartXY(title,xLable,yLable,xyseriescollection,col);
        //lChart.drawLine2DXY("价格VS数量", "数量(MW)", "价格($/MWh)", xyseriescollection, flag, true);
        sc.pack( );
        RefineryUtilities.centerFrameOnScreen( sc );
        sc.setVisible( true );

    }

}
