package com.github.projectflink.avro;

import com.github.projectflink.avro.generated.AvroLineitem;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * Performance tests with avro
 */
/*public class AvroPerf {
	public static void main(final String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		String mode = args[2];

		if(mode.equals("forcekryo")) {
			env.getConfig().enableForceKryo();
		} else if(mode.equals("forceavro")) {
			env.getConfig().enableForceKryo();
			env.getConfig().enableGenericTypeSerializationWithAvro();
		}

		DataSet<AvroLineitem> lineItemFromAvro = env.createInput(
				new AvroInputFormat<AvroLineitem>(new Path(args[0]), AvroLineitem.class));
		DataSet<AvroLineitem> lineItemFromCsv = env.readTextFile(args[1]).map(new Prepare.AvroLineItemMapper());
		CoGroupOperator.CoGroupOperatorSets coGroup = lineItemFromAvro
				.coGroup(lineItemFromCsv);
		CoGroupOperator.CoGroupOperatorSets.CoGroupOperatorSetsPredicate.CoGroupOperatorWithoutFunction el = null;
		KeySelector<AvroLineitem, String> ks = new KeySelector<AvroLineitem, String>() {
			@Override
			public String getKey(AvroLineitem value) throws Exception {
				StringBuilder sb = new StringBuilder();
				sb.append(Long.toString(value.getOrderKey()));
				sb.append(Long.toString(value.getPartKey()));
				sb.append(Long.toString(value.getSupplierKey()));
				sb.append(Long.toString(value.getLineNumber()));
				return sb.toString();
			}
		};
		if(mode.equals("pojo")) {
			el = coGroup.where("orderKey", "partKey", "supplierKey", "lineNumber").equalTo("orderKey", "partKey", "supplierKey", "lineNumber");
		} else if(mode.equals("forcekryo") || mode.equals("forceavro")) {
			el = coGroup.where(ks).equalTo(ks);
		}
		DataSet<String> empty = el.with(new CoGroupFunction<AvroLineitem, AvroLineitem, String>() {
			@Override
			public void coGroup(Iterable<AvroLineitem> avro, Iterable<AvroLineitem> csv, Collector<String> collector) throws Exception {
				Iterator<AvroLineitem> aIt = avro.iterator();
				if (!aIt.hasNext()) {
					throw new RuntimeException("Expected item from Avro input");
				}
				AvroLineitem left = aIt.next();
				if (aIt.hasNext()) {
					throw new RuntimeException("Unexpectedly received two avro records on this side. left=" + left + " next=" + aIt.next());
				}

				Iterator<AvroLineitem> cIt = csv.iterator();
				if (!cIt.hasNext()) {
					throw new RuntimeException("Expected item from CSV input");
				}
				AvroLineitem right = cIt.next();
				if (cIt.hasNext()) {
					throw new RuntimeException("Unexpectedly received two CSV records on this side");
				}
				if (!right.equals(left)) {
					throw new RuntimeException("Records are not equal");
				}
			}
		});
		empty.output(new DiscardingOutputFormat<String>());
		env.execute("Avro perf in mode "+mode);
	}
} */
