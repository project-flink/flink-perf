package com.github.projectflink.avro;

import com.github.projectflink.avro.generated.AvroLineitem;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.AvroOutputFormat;
import org.apache.flink.configuration.Configuration;

import java.text.DateFormat;
import java.text.SimpleDateFormat;


/**
 * Prepare Avro test.
 *
 * Load TPCH data and transform it to Avro
 */
public class Prepare {
	public static void main(final String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> text = env.readTextFile(args[0]);
		DataSet<AvroLineitem> avro = text.map(new AvroLineItemMapper());
		avro.write(new AvroOutputFormat<AvroLineitem>(AvroLineitem.class), args[1]);
		env.execute("Lineitem Text 2 Avro converter");
	}

	public static class AvroLineItemMapper extends RichMapFunction<String, AvroLineitem> {
		DateFormat fs = new SimpleDateFormat("yyyy-MM-dd");

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

		}

		@Override
		public AvroLineitem map(String s) throws Exception {
			getRuntimeContext().getLongCounter("elements").add(1L);

			String[] parts = s.split("\\|");

			return new AvroLineitem(Long.parseLong(parts[0]), // order key
					Long.parseLong(parts[1]), // part key
					Long.parseLong(parts[2]),// supplierKey
					Long.parseLong(parts[3]), // lineNumber
					Long.parseLong(parts[4]), //qty
					Double.parseDouble(parts[5]), //ext
					Double.parseDouble(parts[6]), // dis
					Double.parseDouble(parts[7]), // tax
					parts[8], // ret flag
					parts[9], // status
					fs.parse(parts[10]).getDate(), // ship date
					fs.parse(parts[11]).getDate(), // commit date
					fs.parse(parts[12]).getDate(), // receiptdate
					parts[13], // ship instr
					parts[14], // ship mode
					parts[15] // commit
			);
		}
	}
}
