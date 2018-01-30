package com.Chart;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import java.awt.*;
import java.util.ArrayList;

/**
 * 柱状图
 * Created by hdp on 16-9-13.
 */
public class BarChart extends ApplicationFrame {

    public BarChart(String title) {
        super(title);
    }

    public void drawBarChart(String title, String xLable, String yLable, CategoryDataset dataSet, ArrayList<Paint> style){
        JFreeChart chart= ChartFactory.createBarChart(title,xLable,yLable,dataSet, PlotOrientation.VERTICAL,true,false,false);
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

        plot.setNoDataMessage("无对应的数据，请重新查询。");
        plot.setNoDataMessageFont(titleFont);//字体的大小
        plot.setNoDataMessagePaint(Color.RED);//字体颜色

        ChartPanel chartPanel = new ChartPanel( chart );
        chartPanel.setPreferredSize( new java.awt.Dimension( 560 , 367 ) );
        setContentPane( chartPanel );
    }

    public static void main(String[] args) {
        // TODO Auto-generated method stub

        DefaultCategoryDataset dataset=new DefaultCategoryDataset();
        //dataset.addValue(5, );
        dataset.addValue(1, "workers", "1");
        dataset.addValue(2, "workers", "2");
        dataset.addValue(3, "workers", "3");
        dataset.addValue(8, "workers", "4");
        dataset.addValue(7, "workers", "5");


       // dataset.addValue(10, "farmers", "1");
        //dataset.addValue(20, "farmers", "2");
        //dataset.addValue(18, "farmers", "3");
        //dataset.addValue(50, "farmers", "4");
        //dataset.addValue(33, "farmers", "5");
        //int[] flag={1,0};
        //LineChart lChart=new LineChart("kkkkkkkk");
        //lChart.drawLine2D("测试", "xxxx", "people", dataset, flag, true);
        BarChart chart=new BarChart("KKKKKKK");
        chart.drawBarChart("KLLJ","ASA","ADADSA",dataset,new ArrayList<Paint>());
        chart.pack( );
        RefineryUtilities.centerFrameOnScreen( chart );
        chart.setVisible( true );

    }
}
