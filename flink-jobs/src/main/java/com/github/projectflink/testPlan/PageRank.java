package com.github.projectflink.testPlan;


import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

import java.util.ArrayList;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

@SuppressWarnings("serial")
public class PageRank {

	private static final double DAMPENING_FACTOR = 0.85;

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Tuple1<Long>> pagesInput = env.readCsvFile(pagesInputPath)
			.fieldDelimiter(' ')
			.lineDelimiter("\n")
			.types(Long.class);

		DataSet<Tuple2<Long, Long>> linksInput =  env.readCsvFile(linksInputPath)
			.fieldDelimiter(' ')
			.lineDelimiter("\n")
			.types(Long.class, Long.class);

		// assign initial rank to pages
		DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.
			map(new RankAssigner((1.0d / numPages)));

		// build adjacency list from link input
		DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
			linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

		// set iterative data set
		IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations);

		DataSet<Tuple2<Long, Double>> newRanks = iteration
			// join pages with outgoing edges and distribute rank
			.join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
				// collect and sum ranks
			.groupBy(0).aggregate(SUM, 1)
				// apply dampening factor
			.map(new Dampener(DAMPENING_FACTOR, numPages));

		DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(newRanks);

		// emit result
		finalPageRanks.writeAsCsv(outputPath, "\n", " ");

		// execute program
		env.execute("Basic Page Rank Example");

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * A map function that assigns an initial rank to all pages.
	 */
	public static final class RankAssigner implements MapFunction<Tuple1<Long>, Tuple2<Long, Double>> {
		Tuple2<Long, Double> outPageWithRank;

		public RankAssigner(double rank) {
			this.outPageWithRank = new Tuple2<Long, Double>(-1l, rank);
		}

		@Override
		public Tuple2<Long, Double> map(Tuple1<Long> page) {
			outPageWithRank.f0 = page.f0;
			return outPageWithRank;
		}
	}

	/**
	 * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
	 * originate. Run as a pre-processing step.
	 */
	@ConstantFields("0")
	public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

		private final ArrayList<Long> neighbors = new ArrayList<Long>();

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
			neighbors.clear();
			Long id = 0L;

			for (Tuple2<Long, Long> n : values) {
				id = n.f0;
				neighbors.add(n.f1);
			}
			out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
		}
	}

	/**
	 * Join function that distributes a fraction of a vertex's rank to all neighbors.
	 */
	public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out){
			Long[] neigbors = value.f1.f1;
			double rank = value.f0.f1;
			double rankToDistribute = rank / ((double) neigbors.length);

			for (int i = 0; i < neigbors.length; i++) {
				out.collect(new Tuple2<Long, Double>(neigbors[i], rankToDistribute));
			}
		}
	}

	/**
	 * The function that applies the page rank dampening formula
	 */
	@ConstantFields("0")
	public static final class Dampener implements MapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>> {

		private final double dampening;
		private final double randomJump;

		public Dampener(double dampening, double numVertices) {
			this.dampening = dampening;
			this.randomJump = (1 - dampening) / numVertices;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
			value.f1 = (value.f1 * dampening) + randomJump;
			return value;
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String pagesInputPath = null;
	private static String linksInputPath = null;
	private static String outputPath = null;
	private static long numPages = 0;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] args) {
		if(args.length == 5) {
			pagesInputPath = args[0];
			linksInputPath = args[1];
			outputPath = args[2];
			numPages = Integer.parseInt(args[3]);
			maxIterations = Integer.parseInt(args[4]);
			return true;
		} else {
			System.err.println("Usage: PageRankBasic <pages path> <links path> <output path> <num pages> <num iterations>");
			return false;
		}
	}
}
