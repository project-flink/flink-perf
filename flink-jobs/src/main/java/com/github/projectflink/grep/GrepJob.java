package com.github.projectflink.grep;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.record.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.Arrays;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * This job allows to grep text file for different terms.
 *
 */
public class GrepJob {

	public static void main(final String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		String in = args[0];
		String out = args[1];
		System.err.println("Using input=" + in);
		System.err.println("Using output=" + out);

		String patterns[] = new String[args.length - 2];
		System.arraycopy(args, 2, patterns, 0, args.length - 2);
		System.err.println("Using patterns: " + Arrays.toString(patterns));

		// get input data
		DataSet<String> text = env.readTextFile(args[0]);
		for (int p = 0; p < patterns.length; p++) {
			final String pattern = patterns[p];
			DataSet<String> res = text.filter(new RichFilterFunction<String>() {
				Pattern p = Pattern.compile(pattern);
				LongCounter filterMatches = new LongCounter();
				LongCounter filterRecords = new LongCounter();

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					getRuntimeContext().addAccumulator("filterMatchCount-" + pattern, filterMatches);
					getRuntimeContext().addAccumulator("filterRecordCount-" + pattern, filterRecords);
				}

				@Override
				public boolean filter(String value) throws Exception {
					filterRecords.add(1L);
					if (value == null || value.length() == 0) {
						return false;
					}
					final Matcher m = p.matcher(value);
					if (m.find()) {
						filterMatches.add(1L);
						return true;
					}
					return false;
				}
			}).name("grep for " + pattern);
			res.writeAsText(out + "_" + pattern, FileSystem.WriteMode.OVERWRITE);
		}

		// execute program
		JobExecutionResult jobResult = env.execute("Flink Grep benchmark");
		System.err.println(AccumulatorHelper.getResultsFormated(jobResult.getAllAccumulatorResults()));
	}

/*
	public static class Access {
		public int userId;
		public Date time;
		public int statusCode;
		public String url;
	}

	public class User {
		public int userId;
		public int regionId;
		public Date customerSince;
	}

	private void slides() {
		String logPath = "";

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// get access log
		DataSet<String> logRaw = env.readTextFile(logPath);
		DataSet<Access> accessLog = logRaw.map(new LogToAccessMapper());

		// read user data
		DataSet<User> users =
				env.createInput(new JDBCInputFormat("jdbcIn", "mysql://", "usr", "pw", "SELECT * FROM user"))
						.map(new JDBCToUser());

		// find customers that bought something
		DataSet<Access> goodCustomers = accessLog.filter((access) -> { return access.url.contains("checkout"); });
		DataSet<Access> goodCustomers = accessLog.filter(new FilterFunction<GrepJob.Access>() {
			@Override
			public boolean filter(Access access) throws Exception {
				return access.url.contains("checkout");
			}
		});

		// join them with the users database
		DataSet<Tuple2<Access, User>> campaign = goodCustomers.join(users).where("userId").equalTo("userId");

		campaign.writeAsText(outPath);
		env.execute();
	} */

}
