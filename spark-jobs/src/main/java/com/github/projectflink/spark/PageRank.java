package com.github.projectflink.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;

public class PageRank {

	private static final double DAMPENING_FACTOR = 0.85;


	public static void main(String[] args) {

		if(!parseParameters(args)) {
			return;
		}

		SparkConf conf = new SparkConf().setAppName("Page Rank").setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		// get input data
		JavaRDD<Long> pagesInput = sc
			.textFile(pagesInputPath)
			.map(new Function<String, Long>() {
				@Override
				public Long call(String s) throws Exception {
					return Long.parseLong(s);
				}
			});
		JavaPairRDD<Long, Long> linksInput = sc
			.textFile(linksInputPath)
			.mapToPair(new PairFunction<String, Long, Long>() {
				@Override
				public Tuple2<Long, Long> call(String s) throws Exception {
					String [] line = s.split(" ");
					return new Tuple2<Long, Long>(Long.parseLong(line[0]), Long.parseLong(line[1]));
				}
			});

		// assign initial rank to pages
		JavaPairRDD<Long, Double> pagesWithRanks = pagesInput
			.mapToPair(new RankAssigner(numPages));

		// build adjacency list from link input
		JavaPairRDD<Long, Long[]> adjacencyListInput = linksInput
			.groupByKey()
			.mapToPair(new PairFunction<Tuple2<Long, Iterable<Long>>, Long, Long[]>() {
				private final ArrayList<Long> neighbors = new ArrayList<Long>();
				@Override
				public Tuple2<Long, Long[]> call(Tuple2<Long, Iterable<Long>> t) throws Exception {
					neighbors.clear();
					for (Long n : t._2()) {
						neighbors.add(n);
					}
					return new Tuple2<Long, Long[]>(t._1(), neighbors.toArray(new Long[neighbors.size()]));
				}
			})
			.cache();

		for (int i = 0; i < maxIterations; i++) {
			// join pages with outgoing edges and distribute rank
			JavaPairRDD<Long, Double> rankToDistribute = pagesWithRanks
				.join(adjacencyListInput)
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Tuple2<Double, Long[]>>, Long, Double>() {
					private final ArrayList<Tuple2<Long, Double>> rankDelta = new ArrayList<Tuple2<Long, Double>>();
					@Override
					public Iterable<Tuple2<Long, Double>> call(Tuple2<Long, Tuple2<Double, Long[]>> t) throws Exception {
						rankDelta.clear();
						double r = t._2()._1() / t._2()._2().length;

						for (int j = 0; j < t._2()._2().length; j++) {
							rankDelta.add(new Tuple2<Long, Double>(t._2()._2()[j], r));
						}
						return rankDelta;
					}
				});

			// sum ranks and apply dampening factor
			pagesWithRanks = rankToDistribute
				.reduceByKey(new Function2<Double, Double, Double>() {
					@Override
					public Double call(Double t1, Double t2) throws Exception {
						return t1 + t2;
					}
				})
				.mapToPair(new Dampener(DAMPENING_FACTOR, numPages));
		}

		pagesWithRanks.saveAsTextFile(outputPath);

	}

	public static final class RankAssigner implements PairFunction<Long, Long, Double> {

		private double rank;

		public RankAssigner(Long numVertices){
			this.rank = 1.0d / numVertices;
		}

		@Override
		public Tuple2<Long, Double> call(Long a) throws Exception {
			return new Tuple2<Long, Double>(a, rank);
		}
	}

	public static final class Dampener implements PairFunction<Tuple2<Long, Double>, Long, Double> {

		private final double dampening;
		private final double randomJump;

		public Dampener(double dampening, Long numVertices) {
			this.dampening = dampening;
			this.randomJump = (1 - dampening) / numVertices;
		}
		@Override
		public Tuple2<Long, Double> call(Tuple2<Long, Double> t) throws Exception {
			return new Tuple2<Long, Double>(t._1(), t._2() * dampening + randomJump);
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************


	private static String master;
	private static String pagesInputPath;
	private static String linksInputPath;
	private static String outputPath;
	private static long numPages;
	private static int maxIterations;

	private static boolean parseParameters(String[] programArguments) {

		if(programArguments.length == 6) {
			master = programArguments[0];
			pagesInputPath = programArguments[1];
			linksInputPath = programArguments[2];
			outputPath = programArguments[3];
			numPages = Integer.parseInt(programArguments[4]);
			maxIterations = Integer.parseInt(programArguments[5]);
		} else {
			System.err.println("Usage: PageRankBasic <master> <pages path> <links path> <output path> <num pages> <num iterations>");
			return false;
		}
		return true;
	}
}
