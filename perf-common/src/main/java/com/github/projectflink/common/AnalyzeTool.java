package com.github.projectflink.common;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AnalyzeTool {

	public static void main(String[] args) throws FileNotFoundException {
		Scanner sc = new Scanner(new File(args[0]));
		String l;
		Pattern latencyPattern = Pattern.compile(".*Latency ([0-9]+) ms.*");
		Pattern throughputPattern = Pattern.compile(".*Received ([0-9]+) elements since [0-9]*. Elements per second ([0-9]+), GB received.*");
		Pattern hostPattern = Pattern.compile("Container: .* on (.+).c.astral-sorter-757..*");
		DescriptiveStatistics latencies = new DescriptiveStatistics();
		SummaryStatistics throughputs = new SummaryStatistics();
		String currentHost = null;
		Map<String, DescriptiveStatistics> perHostLat = new HashMap<String, DescriptiveStatistics>();
		Map<String, SummaryStatistics> perHostThr = new HashMap<String, SummaryStatistics>();

		while( sc.hasNextLine()) {
			l = sc.nextLine();
			// ---------- host ---------------
			Matcher hostMatcher = hostPattern.matcher(l);
			if(hostMatcher.matches()) {
				currentHost = hostMatcher.group(1);
				System.err.println("Setting host to "+currentHost);
			}
			// ---------- latency ---------------
			Matcher latencyMatcher = latencyPattern.matcher(l);
			if(latencyMatcher.matches()) {
				int latency = Integer.valueOf(latencyMatcher.group(1));
				latencies.addValue(latency);

				DescriptiveStatistics perHost = perHostLat.get(currentHost);
				if(perHost == null) {
					System.err.println("New descriptive statistics "+currentHost);
					perHost = new DescriptiveStatistics();
					perHostLat.put(currentHost, perHost);
				}
				perHost.addValue(latency);
			}

			// ---------- throughput ---------------
			Matcher tpMatcher = throughputPattern.matcher(l);
			if(tpMatcher.matches()) {
				double eps = Double.valueOf(tpMatcher.group(2));
				throughputs.addValue(eps);

				SummaryStatistics perHost = perHostThr.get(currentHost);
				if(perHost == null) {
					perHost = new SummaryStatistics();
					perHostThr.put(currentHost, perHost);
				}
				perHost.addValue(eps);
			}
		}
		// System.out.println("lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs;");
		System.out.println(latencies.getMean() + ";" + latencies.getPercentile(50) + ";" + latencies.getPercentile(90) + ";" + latencies.getPercentile(95) + ";" + latencies.getPercentile(99)+ ";" + throughputs.getMean() + ";" + throughputs.getMax() + ";" + latencies.getN() + ";" + throughputs.getN());

		for(Map.Entry<String, DescriptiveStatistics> entry : perHostLat.entrySet()) {
			System.err.println("====== "+entry.getKey()+" (entries: "+entry.getValue().getN()+") =======");
			System.err.println("Mean latency " + entry.getValue().getMean());
			System.err.println("Median latency " + entry.getValue().getPercentile(50));
		}
		System.err.println("================= Throughput =====================");
		for(Map.Entry<String, SummaryStatistics> entry : perHostThr.entrySet()) {
			System.err.println("====== "+entry.getKey()+" (entries: "+entry.getValue().getN()+")=======");
			System.err.println("Mean latency " + entry.getValue().getMean());
		}
	}
}
