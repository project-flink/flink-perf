/*
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

package com.github.projectflink.pagerank;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class AdjacencyBuilder {

	public static void main(String[] args) throws Exception {

		String inPath = args.length > 1 ? args[0] : "/data/demodata/pagerank/edges/edges.csv";
		String outPath = args.length > 1 ? args[1] : "/data/demodata/pagerank/adacency_comp";

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, String>> edges = env.readCsvFile(inPath).fieldDelimiter(' ').types(String.class, String.class);

		edges.coGroup(edges).where(0).equalTo(1).with(new AdjacencyCoGroup())
				.writeAsText(outPath, WriteMode.OVERWRITE);

		env.execute();
	}

	public static final class AdjacencyCoGroup implements CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, String> {

		private List<String> list = new ArrayList<String>();
		private StringBuilder bld = new StringBuilder();

		@Override
		public void coGroup(Iterable<Tuple2<String, String>> groupedOnOut, Iterable<Tuple2<String, String>> groupedOnIn, Collector<String> out) {
			String key = null;
			list.clear();

			for (Tuple2<String, String> t : groupedOnOut) {
				key = t.f0;
				list.add(t.f1);
			}

			if (list.isEmpty()) {
				// node with no outgoing edges
				key = groupedOnIn.iterator().next().f1;
			}

			bld.setLength(0);
			bld.append(key).append(' ');
			for (String s : list) {
				bld.append(s).append(' ');
			}

			bld.setLength(bld.length()-1);
			out.collect(bld.toString());
		}
	}
}
