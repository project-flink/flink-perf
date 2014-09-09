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
import java.util.Iterator;
import java.util.List;

public class ConnectedComponents {
	public static void main(String args[]) {
		if(!parseParameters(args)) {
			return;
		}

		SparkConf conf = new SparkConf().setAppName("Connected Components").setMaster(master);
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<Long> vertices = sc
			.textFile(verticesPath)
			.map(new Function<String, Long>() {
				@Override
				public Long call(String s) throws Exception {
					return Long.parseLong(s);
				}
			});

		JavaPairRDD<Long, Long> edges = sc
			.textFile(edgesPath)
			.flatMapToPair(new UndirectedEdge());
		
		JavaPairRDD<Long, Long> cc = vertices.mapToPair(new PairFunction<Long, Long, Long>() {
			@Override
			public Tuple2<Long, Long> call(Long t) throws Exception {
				return new Tuple2<Long, Long>(t, t);
			}
		});

		JavaPairRDD<Long, Long> result = vertices.mapToPair(new PairFunction<Long, Long, Long>() {
			@Override
			public Tuple2<Long, Long> call(Long t) throws Exception {
				return new Tuple2<Long, Long>(t, t);
			}
		});


		for (int i = 0; i < maxIterations; i++) {
			cc = cc.join(edges)
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Long>>, Long, Long>() {
					@Override
					public Tuple2<Long, Long> call(Tuple2<Long, Tuple2<Long, Long>> t) throws Exception {
						return new Tuple2<Long, Long>(t._2()._2(), t._2()._1());
					}
				})
				.reduceByKey(new SmallestNeighbor())
				.join(result)
				.filter(new LessThanCurrent())
				.mapToPair(new NextRoundUpdate());

			result = result.cogroup(cc)
				.mapToPair(new UpdateResult());

			if (cc.count() == 0) {
				break;
			}
		}

		result.saveAsTextFile(outputPath);

	}

	public static final class UndirectedEdge implements PairFlatMapFunction<String, Long, Long> {
		@Override
		public Iterable<Tuple2<Long, Long>> call(String s) throws Exception {
			String [] line = s.split(" ");
			Long v1 = Long.parseLong(line[0]);
			Long v2 = Long.parseLong(line[1]);
			List<Tuple2<Long, Long>> result = new ArrayList<Tuple2<Long, Long>>();
			result.add(new Tuple2<Long, Long>(v1, v2));
			result.add(new Tuple2<Long, Long>(v2, v1));
			return result;
		}
	}

	public static final class SmallestNeighbor implements Function2<Long, Long, Long> {
		@Override
		public Long call(Long t1, Long t2) throws Exception {
			return Math.min(t1, t2);
		}
	}

	public static final class LessThanCurrent implements Function<Tuple2<Long, Tuple2<Long, Long>>, Boolean> {
		@Override
		public Boolean call(Tuple2<Long, Tuple2<Long, Long>> t) throws Exception {
			return t._2()._1().compareTo(t._2()._2()) < 0;
		}
	}

	public static final class NextRoundUpdate implements PairFunction<Tuple2<Long, Tuple2<Long, Long>>, Long, Long> {
		@Override
		public Tuple2<Long, Long> call(Tuple2<Long, Tuple2<Long, Long>> t) throws Exception {
			return new Tuple2<Long, Long>(t._1(), t._2()._1());
		}
	}

	public static final class UpdateResult implements PairFunction<Tuple2<Long, Tuple2<Iterable<Long>, Iterable<Long>>>, Long, Long> {
		@Override
		public Tuple2<Long, Long> call(Tuple2<Long, Tuple2<Iterable<Long>, Iterable<Long>>> t) throws Exception {
			Iterator<Long> t1 = t._2()._1().iterator();
			Iterator<Long> t2 = t._2()._2().iterator();
			if (t2.hasNext()) {
				return new Tuple2<Long, Long>(t._1(), Math.min(t1.next(), t2.next()));
			} else {
				return new Tuple2<Long, Long>(t._1(), t1.next());
			}
		}
	}


	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String master = null;
	private static String verticesPath = null;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;


	private static boolean parseParameters(String[] programArguments) {
		// parse input arguments
		if(programArguments.length == 5) {
			master = programArguments[0];
			verticesPath = programArguments[1];
			edgesPath = programArguments[2];
			outputPath = programArguments[3];
			maxIterations = Integer.parseInt(programArguments[4]);
		} else {
			System.err.println("Usage: Connected Components <master> <vertices path> <edges path> <result path> <max iteration>");
			return false;
		}
		return true;
	}
}
