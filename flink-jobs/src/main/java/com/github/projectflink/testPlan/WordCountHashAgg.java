package com.github.projectflink.testPlan;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files.
 *
 */
@SuppressWarnings("serial")
public class WordCountHashAgg {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<String> text = getTextDataSet(env);

		DataSet<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
						.mapPartition(new HashAggregator())  // "combine"
						.partitionByHash(0)
						.mapPartition(new HashAggregator()); // "reduce"

		// emit result
		if(fileOutput) {
			counts.writeAsCsv(outputPath, "\n", " ");
		} else {
			counts.print();
		}

		// execute program
		env.execute("WordCount Example");
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	public static class HashAggregator extends RichMapPartitionFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
		private Map<String, Integer> words;

		@Override
		public void open(Configuration config) {
			words = new HashMap<String, Integer>();
		}

		@Override
		public void mapPartition(Iterable<Tuple2<String, Integer>> records, Collector<Tuple2
				<String, Integer>> out) throws Exception {
			for (Tuple2<String, Integer> pair: records) {
				Integer value = words.get(pair.f0);
				if(value == null) value = 0;
				words.put(pair.f0, pair.f1 + value);
			}
			Tuple2<String, Integer> result = new Tuple2<String, Integer>();
			for (Map.Entry<String, Integer> pair: words.entrySet()) {
				result.f0 = pair.getKey();
				result.f1 = pair.getValue();
				out.collect(result);
			}
		}
	}
	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if(args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: WordCount <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing WordCount example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: WordCount <text path> <result path>");
		}
		return true;
	}

	private static DataSet<String> getTextDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// get default test text data
			throw new RuntimeException("No build in data");
		}
	}
}

