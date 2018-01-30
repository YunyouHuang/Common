package com.Chart;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PiePlot;
import org.jfree.data.general.DefaultPieDataset;
import org.jfree.data.general.PieDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RefineryUtilities;

import javax.swing.*;
import java.awt.*;

/**
 * Created by hdp on 17-2-23.
 */
public class PieChart extends ApplicationFrame {

    public PieChart(String title) {
        super(title);
        setContentPane(createDemoPanel( ));
    }

    private static PieDataset createDataset( )
    {
        DefaultPieDataset dataset = new DefaultPieDataset( );
        dataset.setValue( "≥1%" , new Double( 19 ) );
        dataset.setValue( "≥0.5% and <1%" , new Double( 42 ) );
        //dataset.setValue( "≥0.1% and <0.5%" , new Double( 17 ) );
        //dataset.setValue( "≥0.01% and <0.1%" , new Double( 1168 ));
        dataset.setValue( "<0.5%" , new Double( 2700 ));
        return dataset;
    }
    private static JFreeChart createChart(PieDataset dataset )
    {
        JFreeChart chart = ChartFactory.createPieChart(
                "The distribution of clusters",  // chart title
                dataset,        // data
                true,           // include legend
                true,
                false);

        chart.setBackgroundPaint(Color.WHITE);
        Font titleFont=new Font("宋体", Font.BOLD, 22);
        //Font font=new Font("宋体", Font.PLAIN, 22);

        Font font=new Font("宋体", Font.BOLD, 22);
        chart.getLegend().setItemFont(font);
        chart.getTitle().setFont(font);

        PiePlot plot = (PiePlot) chart.getPlot();
        //设置饼状图体里的的各个标签字体
        plot.setLabelFont(titleFont);
        plot.setBackgroundPaint(Color.WHITE);

        return chart;
    }
    public static JPanel createDemoPanel( )
    {
        JFreeChart chart = createChart(createDataset( ) );
        return new ChartPanel( chart );
    }
    public static void main( String[ ] args )
    {
        PieChart demo = new PieChart( "Mobile Sales" );
        demo.setSize( 560 , 367 );
        RefineryUtilities.centerFrameOnScreen( demo );
        demo.setVisible( true );
    }
}
