package eu.stratosphere.test.testPlan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.hadoopcompatibility.HadoopDataSource;
import eu.stratosphere.hadoopcompatibility.datatypes.WritableWrapperConverter;
import eu.stratosphere.test.testPlan.LargeTestPlan.CheckHadoop;
import eu.stratosphere.test.testPlan.LargeTestPlan.CheckHadoopWrapper;
import eu.stratosphere.test.testPlan.LargeTestPlan.FailOutOutputFormat;

public class SequenceFileTest implements Program {

	public Plan getPlan(String... args) {
		String sequenceFileInputPath = args[0];
		int dop = Integer.parseInt(args[1]);
		
		JobConf jobConf = new JobConf();
		FileInputFormat.addInputPath(jobConf, new Path(sequenceFileInputPath));
		//  with Stratosphere type converter
		HadoopDataSource hdsrc = new HadoopDataSource(new SequenceFileInputFormat<LongWritable, Text>(), jobConf, "Sequencefile");
		MapOperator checkHDsrc = MapOperator.builder(CheckHadoop.class).input(hdsrc).name("Check HDSrc output").build();
		
		HadoopDataSource hdsrcWrapperConverter = new HadoopDataSource(new SequenceFileInputFormat<LongWritable, Text>(), jobConf, "Sequencefile (WritableWrapper)", new WritableWrapperConverter());
		MapOperator checkHDsrcWrapperConverter = MapOperator.builder(CheckHadoopWrapper.class).input(hdsrcWrapperConverter).name("Check HDSrc output").build();
		// END: TEST 8

		// don't use this for serious output. 
		FileDataSink fakeSink = new FileDataSink(FailOutOutputFormat.class, "file:///tmp/fakeOut", "fake out");
		fakeSink.addInput(checkHDsrc);
		fakeSink.addInput(checkHDsrcWrapperConverter);
		
		Plan p = new Plan(fakeSink, "Sequencefile Test");
		p.setDefaultParallelism(dop);
		return p;
	}

	public static void main(String[] args) throws Exception {
		SequenceFileTest sqT = new SequenceFileTest();
		JobExecutionResult res = LocalExecutor.execute(sqT.getPlan("/home/twalthr/repo/test/seq"));
		System.err.println("Result:\n"+AccumulatorHelper.getResultsFormated(res.getAllAccumulatorResults()));
	}

}
