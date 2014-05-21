package eu.stratosphere.test.testPlan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.cache.DistributedCache;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * Test pull request https://github.com/stratosphere/stratosphere/pull/564.
 * 
 * @author robert
 *
 */
public class DistributedCacheTest implements Program  {
	private static final long serialVersionUID = 1L;

	public static class PoorJoin extends MapFunction {
		private static final long serialVersionUID = 1L;
		List<String> cachedLines = new LinkedList<String>();
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			DistributedCache c = getRuntimeContext().getDistributedCache();
			BufferedReader br = new BufferedReader(new FileReader(c.getFile("cacheFile")));
			String line;
			while ((line = br.readLine()) != null) {
				cachedLines.add(new String(line));
			}
			br.close();
		}
		@Override
		public void map(Record record, Collector<Record> collector) {
			for(String line: cachedLines) {
				StringValue maLine = record.getField(0, StringValue.class);
				maLine.setValue(maLine.getValue()+" : "+line);
				record.setField(0, maLine);
				collector.collect(record);
			}
		}
	}



	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);

		FileDataSource source = new FileDataSource(new TextInputFormat(), args[2], "Input Lines");
		MapOperator mapper = MapOperator.builder(new PoorJoin())
			.input(source)
			.name("Cross or so")
			.build();
		@SuppressWarnings("unchecked")
		FileDataSink out = new FileDataSink(new CsvOutputFormat("\n", " ", StringValue.class), args[3], mapper, "Bla");
		
		Plan plan = new Plan(out, "DistCacheTest");
		try {
			plan.registerCachedFile(args[1], "cacheFile");
		} catch (Exception e) {
			e.printStackTrace(); 
		}
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}


	public String getDescription() {
		return "Parameters: <numSubStasks> <distCacheInput> <mapInput> <output>";
	}

	
	public static void main(String[] args) throws Exception {
		DistributedCacheTest wc = new DistributedCacheTest();
		
		if (args.length < 3) {
			System.err.println(wc.getDescription());
			System.exit(1);
		}
		
		Plan plan = wc.getPlan(args);
		
		// This will execute the word-count embedded in a local context. replace this line by the commented
		// succeeding line to send the job to a local installation or to a cluster for execution
		JobExecutionResult result = LocalExecutor.execute(plan);
		System.err.println("Total runtime: " + result.getNetRuntime());
//		PlanExecutor ex = new RemoteExecutor("localhost", 6123, "stratosphere-java-examples-0.4-WordCount.jar");
//		ex.executePlan(plan);
	}
}
