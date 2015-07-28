package com.github.projectflink.common;


import java.awt.BasicStroke;
import java.awt.Dimension;
import java.io.File;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import org.jfree.chart.ChartColor;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.plot.SeriesRenderingOrder;
import org.jfree.chart.plot.ValueMarker;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.util.RelativeDateFormat;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.Minute;
import org.jfree.data.time.MovingAverage;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.ApplicationFrame;
import org.jfree.ui.RectangleInsets;
import org.jfree.ui.RefineryUtilities;

public class KafkaFT extends ApplicationFrame {

	private static final long serialVersionUID = 1L;

	public KafkaFT(String s, String generatorLogPath, String stateMachineLogPath) throws Exception {
		super(s);
		WhyOO res = createDataset(generatorLogPath, stateMachineLogPath);
		JFreeChart jfreechart = createChart(res.ds);

		// add kill events:
		for(Long killEventTime: res.killEventTime) {
			addKillEvent(jfreechart.getXYPlot(), killEventTime);
		}

		// add illegal state trans events:
		for(Long illegalEventTime: res.illegalStates) {
			addIllegalEvent(jfreechart.getXYPlot(), illegalEventTime);
		}



		ChartPanel chartpanel = new ChartPanel(jfreechart);
		chartpanel.setPreferredSize(new Dimension(1200, 600));
		setContentPane(chartpanel);
	}

	private static void addKillEvent(XYPlot xyplot, long pos) {
		ValueMarker vm = new ValueMarker(pos);
		vm.setPaint(ChartColor.VERY_DARK_GREEN);
		vm.setLabelOffset(new RectangleInsets(10.0D, 1.0D, 1.0D, 1.0D));
		vm.setLabel("Container Kill Event");
		vm.setStroke(new BasicStroke(2));
		xyplot.addDomainMarker(vm);
	}

	private static void addIllegalEvent(XYPlot xyplot, long pos) {
		ValueMarker vm = new ValueMarker(pos);
		vm.setPaint(ChartColor.LIGHT_YELLOW);
		vm.setLabelOffset(new RectangleInsets(10.0D, 1.0D, 1.0D, 1.0D));
		vm.setLabel("Illegal State");
		vm.setStroke(new BasicStroke(2));
		xyplot.addDomainMarker(vm);
	}


	private static JFreeChart createChart(XYDataset xydataset) {
		JFreeChart jfreechart = ChartFactory.createTimeSeriesChart("Flink Exactly-Once on Kafka with YARN Chaos Monkey", "Date", "Value", xydataset, true, true, false);
		XYPlot xyplot = (XYPlot) jfreechart.getPlot();

		XYLineAndShapeRenderer r0 = (XYLineAndShapeRenderer) xyplot.getRenderer(0);

		// draw data points as points
		r0.setSeriesShapesVisible(2, true);
		r0.setSeriesLinesVisible(2, true);
		// order elements as assed
		xyplot.setSeriesRenderingOrder(SeriesRenderingOrder.FORWARD);

		DateAxis dateaxis = (DateAxis) xyplot.getDomainAxis();

		Number first = xydataset.getX(0, 0);
		Minute minute = new Minute(new Date((Long)first));
		System.out.println("first = "+first);
		RelativeDateFormat relativedateformat = new RelativeDateFormat(minute.getFirstMillisecond());
		relativedateformat.setSecondFormatter(new DecimalFormat("00"));
		dateaxis.setDateFormatOverride(relativedateformat);


		//dateaxis.setDateFormatOverride(new SimpleDateFormat("mm:ss"));
		ValueAxis valueaxis = xyplot.getRangeAxis();
		valueaxis.setAutoRangeMinimumSize(1.0D);
		valueaxis.setLabel("Elements/Core");

		xyplot.getRenderer().setSeriesPaint(2, ChartColor.DARK_MAGENTA);
		return jfreechart;
	}

	public static class WhyOO {
		public XYDataset ds;
		public List<Long> killEventTime;
		public List<Long> illegalStates;
	}
	private static WhyOO createDataset(String generatorLogPath, String stateMachineLogPath) throws Exception {
		WhyOO res = new WhyOO();
		res.killEventTime = new ArrayList<Long>();
		res.illegalStates = new ArrayList<Long>();

		TimeSeries generatorTimeseries = new TimeSeries("Data Generator");

		{
			Scanner sc = new Scanner(new File(generatorLogPath));
			String l;
			Pattern throughputPattern = Pattern.compile("([0-9:,]+) INFO.*generated.*That's ([0-9.]+) elements\\/second\\/core.*");
			SimpleDateFormat dateParser = new SimpleDateFormat("HH:mm:ss,SSS");
			while (sc.hasNextLine()) {
				l = sc.nextLine();
				Matcher tpMatcher = throughputPattern.matcher(l);
				if (tpMatcher.matches()) {
					String time = tpMatcher.group(1);
					double eps = Double.valueOf(tpMatcher.group(2));
					generatorTimeseries.addOrUpdate(new Millisecond(dateParser.parse(time)), eps);
				}
			}
		}

		TimeSeries consumerTimeseries = new TimeSeries("State Machine");

		{
			Scanner sc = new Scanner(new File(stateMachineLogPath));
			String l;
			Pattern throughputPattern = Pattern.compile("([0-9:,]+) INFO.*That's ([0-9.]+) elements\\/second\\/core.*");
			Pattern failPattern = Pattern.compile("([0-9:,]+) ERROR .* failed.*");
			Pattern illegalStatePattern = Pattern.compile("([0-9:,]+) INFO .*Detected invalid state transition ALERT.*");
			SimpleDateFormat dateParser = new SimpleDateFormat("HH:mm:ss,SSS");
			while (sc.hasNextLine()) {
				l = sc.nextLine();
				Matcher tpMatcher = throughputPattern.matcher(l);
				if (tpMatcher.matches()) {
					String time = tpMatcher.group(1);
					double eps = Double.valueOf(tpMatcher.group(2));
					consumerTimeseries.addOrUpdate(new Millisecond(dateParser.parse(time)), eps);
				}
				Matcher failMatcher = failPattern.matcher(l);
				if(failMatcher.matches()) {
					String time = failMatcher.group(1);
					res.killEventTime.add(dateParser.parse(time).getTime());
				}
				Matcher illegalMatcher = illegalStatePattern.matcher(l);
				if(illegalMatcher.matches()) {
					String time = illegalMatcher.group(1);
					res.illegalStates.add(dateParser.parse(time).getTime());
				}
			}
		}

		TimeSeriesCollection timeseriescollection = new TimeSeriesCollection();
		// createPointMovingAverage(TimeSeries source, java.lang.String name, int pointCount)

		timeseriescollection.addSeries(generatorTimeseries);
		timeseriescollection.addSeries(MovingAverage.createPointMovingAverage(generatorTimeseries, "Data Generator (Avg)", 50));

		//timeseriescollection.addSeries(MovingAverage.createMovingAverage(generatorTimeseries, "whyName?", 20, 20));
		// timeseriescollection.addSeries(consumerTimeseries);
		timeseriescollection.addSeries(MovingAverage.createMovingAverage(consumerTimeseries, "State Machine (Avg)", 1000,0));

		System.out.println("Generator elements " + generatorTimeseries.getItemCount());
		System.out.println("Consumer elements " + consumerTimeseries.getItemCount());
		res.ds = timeseriescollection;

		return res;
	}

/*	public static JPanel createDemoPanel() {
		JFreeChart jfreechart = createChart(createDataset());
		return new ChartPanel(jfreechart);
	} */

	public static void main(String args[]) throws Exception {
		KafkaFT timeseriesdemo6 = new KafkaFT("Flink Exactly Once Processing", args[0], args[1]);
		timeseriesdemo6.pack();
		RefineryUtilities.centerFrameOnScreen(timeseriesdemo6);
		timeseriesdemo6.setVisible(true);
	}
}
