package com.github.projectflink.pagerank;

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


import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class PageRankStephan {

	public static void main(String[] args) throws Exception {

		String adjacencyPath = args[0]; //"/data/demodata/pagerank/adjacency/adjacency.csv";
		String outpath = args[1]; //"/home/cicero/Desktop/out.txt";


		int numIterations = Integer.valueOf(args[2]);
		long numVertices = Integer.valueOf(args[3]);


		final double threshold = 0.005 / numVertices;


		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Long, long[]>> adjacency = env.readTextFile(adjacencyPath).map(new AdjacencyBuilder());
		DataSet<Tuple2<Long, long[]>> adjacency2 = env.readTextFile(adjacencyPath).map(new AdjacencyBuilder());

		DataSet<Tuple2<Long, Double>> initialRanks = adjacency.map(new VertexInitializer(1.0 / numVertices));


		IterativeDataSet<Tuple2<Long, Double>> iteration = initialRanks.iterate(numIterations);

		DataSet<Tuple2<Long, Double>> newRanks = iteration
				.join(adjacency2, JoinHint.REPARTITION_HASH_SECOND).where(0).equalTo(0).with(new RankDistributor(0.85, numVertices))
				.groupBy(0)
				.reduceGroup(new Adder());

		DataSet<Integer> tc = iteration.join(newRanks).where(0).equalTo(0).with(new FlatJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Integer>() {
			@Override
			public void join(Tuple2<Long, Double> longDoubleTuple2, Tuple2<Long, Double> longDoubleTuple22, Collector<Integer> collector) throws Exception {
				double delta = Math.abs(longDoubleTuple2.f1 - longDoubleTuple22.f1);
				if(delta > threshold) {
					collector.collect(1);
				}
			}
		});

		iteration.closeWith(newRanks, tc).writeAsCsv(outpath+"_fastbulk", WriteMode.OVERWRITE);

//		System.out.println(env.getExecutionPlan());

		env.execute("Page Rank Optimized");
	}



	public static final class AdjacencyBuilder implements MapFunction<String, Tuple2<Long, long[]>> {

		@Override
		public Tuple2<Long, long[]> map(String value) throws Exception {
			String[] parts = value.split(" ");
			if (parts.length < 1) {
				throw new Exception("Malformed line: " + value);
			}

			long id = Long.parseLong(parts[0]);
			long[] targets = new long[parts.length - 1];
			for (int i = 0; i < targets.length; i++) {
				targets[i] = Long.parseLong(parts[i+1]);
			}
			return new Tuple2<Long, long[]>(id, targets);
		}
	}

	public static final class VertexInitializer implements MapFunction<Tuple2<Long, long[]>, Tuple2<Long, Double>> {

		private final Double initialRank;

		public VertexInitializer(double initialRank) {
			this.initialRank = initialRank;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, long[]> value) {
			return new Tuple2<Long, Double>(value.f0, initialRank);
		}
	}

	public static final class RankDistributor implements FlatJoinFunction<Tuple2<Long, Double>, Tuple2<Long, long[]>, Tuple2<Long, Double>> {

		private final Tuple2<Long, Double> tuple = new Tuple2<Long, Double>();

		private final double dampeningFactor;
		private final long numVertices;


		public RankDistributor(double dampeningFactor, long numVertices) {
			this.dampeningFactor = dampeningFactor;
			this.numVertices = numVertices;
		}


		@Override
		public void join(Tuple2<Long, Double> page, Tuple2<Long, long[]> neighbors, Collector<Tuple2<Long, Double>> out) {
			long[] targets = neighbors.f1;

			double rankPerTarget = dampeningFactor * page.f1 / targets.length;
			double randomJump = (1-dampeningFactor) / numVertices;

			// emit random jump to self
			tuple.f0 = page.f0;
			tuple.f1 = randomJump;
			out.collect(tuple);

			tuple.f1 = rankPerTarget;
			for (long target : targets) {
				tuple.f0 = target;
				out.collect(tuple);
			}
		}
	}

	@ForwardedFields("0")
	public static final class Adder extends RichGroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public void reduce(Iterable<Tuple2<Long, Double>> values, Collector<Tuple2<Long, Double>> out)  {
			Long key = null;
			double agg = 0.0;

			for (Tuple2<Long, Double> t : values) {
				key = t.f0;
				agg += t.f1;
			}

			out.collect(new Tuple2<Long, Double>(key, agg));
		}
	}
}

