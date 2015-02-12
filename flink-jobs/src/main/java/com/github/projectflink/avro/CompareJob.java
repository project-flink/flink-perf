package com.github.projectflink.avro;

import com.github.projectflink.avro.generated.AvroLineitem;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 *
 * Required steps:
 * - Run Generate Lineitems: ./flink run -v -p 152 -c com.github.projectflink.avro.GenerateLineitems ../../testjob/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar -p 152 -o hdfs:///user/robert/datasets/tpch1/
 * - Run Prepare ./flink run -v -p 152 -c com.github.projectflink.avro.Prepare ../../testjob/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar hdfs:///user/robert/datasets/tpch1/lineitems.csv hdfs:///user/robert/datasets/tpch1-avro/
 * - Run Compare. ./flink run -v -p 152 -c com.github.projectflink.avro.CompareJob  ../../testjob/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar hdfs:///user/robert/datasets/tpch1-avro/ hdfs:///user/robert/datasets/tpch1/lineitems.csv
 *
 *
 * This job reads the Lineitem file from text and avro and compares if they match.
 */
public class CompareJob {
	public static void main(final String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<AvroLineitem> lineItemFromAvro = env.createInput(
				new AvroInputFormat<AvroLineitem>(new Path(args[0]), AvroLineitem.class));
		DataSet<AvroLineitem> lineItemFromCsv = env.readTextFile(args[1]).map(new Prepare.AvroLineItemMapper());
		DataSet<String> empty = lineItemFromAvro
			.coGroup(lineItemFromCsv).where("orderKey", "partKey", "supplierKey", "lineNumber").equalTo("orderKey", "partKey", "supplierKey", "lineNumber").with(new CoGroupFunction<AvroLineitem, AvroLineitem, String>() {
				@Override
				public void coGroup(Iterable<AvroLineitem> avro, Iterable<AvroLineitem> csv, Collector<String> collector) throws Exception {
					Iterator<AvroLineitem> aIt = avro.iterator();
					if(!aIt.hasNext()) {
						throw new RuntimeException("Expected item from Avro input");
					}
					AvroLineitem left = aIt.next();
					if(aIt.hasNext()) {
						throw new RuntimeException("Unexpectedly received two avro records on this side. left="+left+" next="+aIt.next());
					}

					Iterator<AvroLineitem> cIt = csv.iterator();
					if(!cIt.hasNext()) {
						throw new RuntimeException("Expected item from CSV input");
					}
					AvroLineitem right = cIt.next();
					if(cIt.hasNext()) {
						throw new RuntimeException("Unexpectedly received two CSV records on this side");
					}
					if(!right.equals(left)) {
						throw new RuntimeException("Records are not equal");
					}
				}
			});
		empty.output(new DiscardingOutputFormat<String>());
		env.execute("Compare Job");
	}
}
