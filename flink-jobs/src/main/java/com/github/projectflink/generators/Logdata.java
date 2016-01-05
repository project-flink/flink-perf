package com.github.projectflink.generators;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.CollectionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;


/**
 * TODO: There is something wrong with the parallelization of the generator.
 */

public class Logdata {
	private static String[] requestType = {"GET", "POST", "PUT", "DELETE"};


	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = /*new CollectionEnvironment(); */ ExecutionEnvironment.getExecutionEnvironment();

		int dop = Integer.valueOf(args[0]);
		String outPath = args[1];
		long finalSizeGB = Integer.valueOf(args[2]);
		final long bytesPerMapper = ((finalSizeGB * 1024 * 1024 * 1024) / dop);
		System.err.println("Generating Log data with the following properties:\n"
				+ "dop="+dop+" outPath="+outPath+" finalSizeGB="+finalSizeGB+" bytesPerMapper="+bytesPerMapper);

		DataSet<Long> empty = env.generateSequence(1, dop);
		empty.print();
		DataSet<String> logLine = empty.flatMap(new FlatMapFunction<Long, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void flatMap(Long value, Collector<String> out) throws Exception {
				System.err.println("val = "+value);

				Random rnd = new Utils.XORShiftRandom();
				StringBuffer sb = new StringBuffer();
				long bytesGenerated = 0;
				while(true) {
					// write ip:
					sb.append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255)).append('.').append(rnd.nextInt(255));
					sb.append(" - - ["); // some spaces
					sb.append( (new Date(Math.abs(rnd.nextLong())).toString()));
					sb.append("] \"");
					sb.append(requestType[rnd.nextInt(requestType.length-1)]);
					sb.append(' ');
					if(rnd.nextBoolean()) {
						// access to album
						sb.append("/album.php?picture=").append(rnd.nextInt());
					} else {
						// access search
						sb.append("/search.php?term=");
						int terms = rnd.nextInt(8);
						for(int i = 0; i < terms; i++) {
							sb.append(Utils.getRandomRealWord(rnd)).append('+');
						}
					}
					sb.append(" HTTP/1.1\" ").append(Utils.getRandomUA(rnd));
					/*if(sb.charAt(sb.length()-1) != '\n') {
						sb.append('\n');
					} */
					final String str = sb.toString();
					sb.delete(0, sb.length());
					bytesGenerated += str.length();
					out.collect(str);
					if(bytesGenerated > bytesPerMapper) {
						break;
					}
				}
			}
		}).setParallelism(dop);
		logLine.writeAsText(outPath, WriteMode.OVERWRITE);
		env.setParallelism(dop);
		env.execute("Flink Distributed Log Data Generator");
	}
}
