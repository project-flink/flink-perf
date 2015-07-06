package com.github.projectflink.common;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {

	public static void main(String[] args) throws FileNotFoundException {
		Scanner sc = new Scanner(new File(args[0]));
		String l;
		Pattern latencyPattern = Pattern.compile(".*Latency ([0-9]+) ms.*");
		Pattern throughputPattern = Pattern.compile(".*Received ([0-9]+) elements since [0-9]*. Elements per second ([0-9]+), GB received.*");

		DescriptiveStatistics latencies = new DescriptiveStatistics();
		SummaryStatistics throughputs = new SummaryStatistics();
		while( sc.hasNextLine()) {
			l = sc.nextLine();
			// ---------- latency ---------------
			Matcher latencyMatcher = latencyPattern.matcher(l);
			if(latencyMatcher.matches()) {
				int latency = Integer.valueOf(latencyMatcher.group(1));
				latencies.addValue(latency);
			}

			// ---------- throughput ---------------
			Matcher tpMatcher = throughputPattern.matcher(l);
			if(tpMatcher.matches()) {
				double eps = Double.valueOf(tpMatcher.group(2));
				throughputs.addValue(eps);
			}
		}
		// System.out.println("lat-mean;lat-median;lat-90percentile;throughput-mean;throughput-max;latencies;throughputs;");
		System.out.println(latencies.getMean()+";"+latencies.getPercentile(50)+";"+latencies.getPercentile(90)+";"+throughputs.getMean()+";"+throughputs.getMax()+";"+latencies.getN()+";"+throughputs.getN());

	}
}
