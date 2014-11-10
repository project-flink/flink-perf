package com.github.projectflink;

import java.io.BufferedReader;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Little stupid util to get the iteration times
 */
public class IterationParser {
	public enum STATE {

	}
	public static void mainReadDelta(String[] args) throws Exception {
		try {
			BufferedReader br = new BufferedReader(new FileReader("/home/robert/flink-workdir/flink-perf/flink-rob-testjob-taskmanager-cloud-14.log.3"));
			String line;
			int iteration = 1;

			Date iterationStart = null;
			Date iterationEnd = null;
			//	long iterationStart = 0;

			while ((line = br.readLine()) != null) {
				// System.err.println("line = "+line);
				// find first iteration start
				if (iterationStart == null && line.contains("starting iteration [" + iteration + "]")) {
				//	System.err.println("found start");
					iterationStart = getDate(line);
				}
				// find last iteration end
				if(line.contains("done in iteration ["+iteration+"]")) {
				//	System.err.println("found end");
					iterationEnd = getDate(line);
					long duration = iterationEnd.getTime() - iterationStart.getTime();
					System.err.println(iteration+","+ duration);
					iteration++;
					iterationStart = null;
				}
			}
			br.close();
		} catch (Throwable t) {
			System.err.println("ex : "+t.getMessage());
			t.printStackTrace();
		}
	}


	// read bulk
	public static void main(String[] args) throws Exception {
		try {
			BufferedReader br = new BufferedReader(new FileReader("/home/robert/flink-workdir/flink-perf/flink-rob-testjob-taskmanager-cloud-14.log.3"));
			String line;
			int iteration = 1;

			Date iterationStart = null;
			Date iterationEnd = null;
			//	long iterationStart = 0;

			while ((line = br.readLine()) != null) {
				// System.err.println("line = "+line);
				if(!line.contains("Bulk")) continue;
				// find first iteration start
				if (iterationStart == null && line.contains("starting iteration [" + iteration + "]")) {
					//	System.err.println("found start");
					iterationStart = getDate(line);
				}
				// find last iteration end
				if(line.contains("finishing iteration ["+(iteration)+"]")) {
					//	System.err.println("found end");
					iterationEnd = getDate(line);
					long duration = iterationEnd.getTime() - iterationStart.getTime();
					System.err.println(iteration+","+ duration);
					iteration++;
					iterationStart = null;
				}
			}
			br.close();
		} catch (Throwable t) {
			System.err.println("ex : "+t.getMessage());
			t.printStackTrace();
		}
	}


	private static Date getDate(String line) throws ParseException {
		String[] sp = line.split(" ");
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss,SSS");
		return sdf.parse(sp[0]);
	}
}
