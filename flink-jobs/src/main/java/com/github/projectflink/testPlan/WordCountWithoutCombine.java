/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package com.github.projectflink.testPlan;

import java.util.Iterator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


@SuppressWarnings("serial")
public class WordCountWithoutCombine {
	
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
		DataSet<String> text = env.readTextFile(textPath);
		
		DataSet<Tuple2<String, Integer>> counts = 
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer()).filter(new FilterFunction<Tuple2<String,Integer>>() {
					@Override
					public boolean filter(Tuple2<String, Integer> value) throws Exception {
						return !value.f1.equals("");
					}
				})
				// group by the tuple field "0" and sum up tuple field "1"
				.groupBy(0)
				.reduceGroup(new GroupReduceFunction<Tuple2<String,Integer>, Tuple2<String, Integer>>() {
					@Override
					public void reduce(
							Iterable<Tuple2<String, Integer>> valuesIt,
							Collector<Tuple2<String, Integer>> out) throws Exception {
						Iterator<Tuple2<String, Integer>> values = valuesIt.iterator();
						int count = 0;
						Tuple2<String, Integer> val = null; // this always works because the iterator always has something.
						while(values.hasNext()) {
							val = values.next();
							count += val.f1;
						}
						val.f1 = count;
						out.collect(val);
					}
				});
		
		counts.writeAsText(outputPath);
		// counts.writeAsCsv(outputPath, "\n", " ");
		
		// execute program
		env.execute("WordCountWithoutcombine");
	}
	
	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************
	
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
	
	private static String textPath;
	private static String outputPath;
	
	private static boolean parseParameters(String[] args) {
		
		if(args.length > 0) {
			// parse input arguments
			if(args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Wrong argument count. got "+args.length);
				System.err.println("Usage: WordCountWithoutCombine <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing WordCount example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: WordCount <text path> <result path>");
		}
		return true;
	}
}
