package com.github.projectflink.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class GrepCaching {

	public static void main(String[] args) {
		String master = args[0];
		String inFile = args[1];
		String outFile = args[2];

		String patterns[] = new String[args.length-3];
		System.arraycopy(args, 3, patterns, 0, args.length - 3);
		System.err.println("Starting spark with master="+master+" in="+inFile);
		System.err.println("Using patterns: "+ Arrays.toString(patterns));

		SparkConf conf = new SparkConf().setAppName("Grep job").setMaster(master).set("spark.hadoop.validateOutputSpecs", "false");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> file = sc.textFile(inFile).persist(StorageLevel.MEMORY_AND_DISK());
		for(int p = 0; p < patterns.length; p++) {
			final String pattern = patterns[p];
			JavaRDD<String> res = file.filter(new Function<String, Boolean>() {
				private static final long serialVersionUID = 1L;
				Pattern p = Pattern.compile(pattern);

				@Override
				public Boolean call(String value) throws Exception {
					if (value == null || value.length() == 0) {
						return false;
					}
					final Matcher m = p.matcher(value);
					if (m.find()) {
						return true;
					}
					return false;
				}
			});
			res.saveAsTextFile(outFile+"_"+pattern);
		}
	}
}
