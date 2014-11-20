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

package com.github.projectflink.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;


public class KMeansArbitraryDimension {

	public static void main(String[] args) {

		if(!parseParameters(args)) {
			return;
		}

		SparkConf conf = new SparkConf().setAppName("KMeans Multi-Dimension").setMaster(master).set("spark.hadoop.validateOutputSpecs", "false");
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		// conf.set("spark.kryo.registrator", ScalaRegistrator.class.getCanonicalName());
		conf.set("spark.kryo.registrator", MyRegistrator.class.getCanonicalName());



		JavaSparkContext sc = new JavaSparkContext(conf);

		// ================================ Standard KMeans =============================

		JavaRDD<Point> points = sc
			.textFile(pointsPath, dop)
			.map(new ConvertToPoint()).cache();
		for(Point p : points.collect()) {
		//	System.err.println("point = "+p);
		}
		points.saveAsTextFile(outputPath+"points_after_readin");

		JavaPairRDD<Integer, Point> kCenters = sc
			.textFile(centersPath)
			.mapToPair(new ConvertToCentroid());

		for(int i=0; i<numIterations; ++i) {
			Broadcast<List<Tuple2<Integer, Point>>> brCenters = sc.broadcast(kCenters.collect());

			kCenters = points
				// compute closest centroid for each point
				.mapToPair(new SelectNearestCentroid(brCenters))
				// count and sum point coordinates for each centroid
				.mapToPair(new CountAppender())
				.reduceByKey(new CentroidSum())
				// calculate the mean( the new center ) of each cluster
				.mapToPair(new CentroidAverage());

			brCenters.unpersist();
		}

		Broadcast<List<Tuple2<Integer, Point>>> brCenters = sc.broadcast(kCenters.collect());
		JavaPairRDD<Integer, Point> clusteredPoints = points.mapToPair(new SelectNearestCentroid(brCenters));

		clusteredPoints.saveAsTextFile(outputPath);
	}

	/** Convert String value into data point **/
	public static final class ConvertToPoint implements Function<String, Point> {

		@Override
		public Point call(String s) throws Exception {
			String [] line = s.split(" ");
			double [] points = new double[line.length];
			for (int i = 0; i < line.length; i++) {
				points[i] = Double.parseDouble(line[i]);
			}
			return new Point(points);
		}
	}

	/** Convert String value into data centroid **/
	public static final class ConvertToCentroid implements PairFunction<String, Integer, Point> {

		@Override
		public Tuple2<Integer, Point> call(String s) throws Exception {
			String [] line = s.split(" ");
			int id = Integer.parseInt(line[0]);
			double [] points = new double[line.length - 1];
			for (int i = 1; i < line.length; i++) {
				points[i - 1] = Double.parseDouble(line[i]);
			}
			return new Tuple2<Integer, Point>(id, new Point(points));
		}
	}

	/**
	 * Assign each point to its closest center
	 *
	 */
	public static final class SelectNearestCentroid implements PairFunction<Point, Integer, Point> {
		List<Tuple2<Integer, Point>> brCenters;

		public SelectNearestCentroid(Broadcast<List<Tuple2<Integer, Point>>> brCenters) {
			this.brCenters = brCenters.getValue();
		}

		public Tuple2<Integer, Point> call(Point v1) throws Exception {
			System.err.println("sel nearest center in "+v1);
			double minDistance = Double.MAX_VALUE;
			int centerId = 0;

			for(Tuple2<Integer, Point> c : brCenters) {
				double d = v1.euclideanDistance(c._2());
				if(minDistance > d) {
					minDistance = d;
					centerId = c._1();
				}
			}
			return new Tuple2<Integer, Point>(centerId, v1);
		}
	}

	/**
	 * Appends a count variable to the tuple.
	 */
	public static final class CountAppender implements PairFunction<Tuple2<Integer, Point>, Integer, Tuple3<Point, Long, Integer>> {

		@Override
		public Tuple2<Integer, Tuple3<Point, Long, Integer>> call(Tuple2<Integer, Point> t) throws Exception {
			Tuple2<Integer, Tuple3<Point, Long, Integer>> r = new Tuple2<Integer, Tuple3<Point, Long, Integer>>(t._1(), new Tuple3<Point, Long, Integer>(t._2(), 1L, t._1()));
		//	System.err.println("spark Count appender "+r);
			return r;
		}
	}


	/**
	 * Aggregate(sum) all the points in each cluster for calculating mean
	 *
	 */
	public static final class CentroidSum implements Function2<Tuple3<Point, Long, Integer>, Tuple3<Point, Long, Integer>, Tuple3<Point, Long, Integer>> {

		@Override
		public Tuple3<Point, Long, Integer> call(Tuple3<Point, Long, Integer> v1, Tuple3<Point, Long, Integer> v2) throws Exception {
		//	System.err.println("sp a1 = "+v1+" a2 = "+v2);
			Tuple3<Point, Long, Integer> r = new Tuple3<Point, Long, Integer>(v1._1().add(v2._1()), v1._2() + v2._2(), v1._3());
		//	System.err.println("spark accu out = "+r+" key = "+v1._3()+" (and "+v2._3()+")");
			return r;
		}
	}

	/**
	 * Calculate the mean(new center) of the cluster ( sum of points / number of points )
	 *
	 */
	public static final class CentroidAverage implements PairFunction<Tuple2<Integer, Tuple3<Point, Long, Integer>>, Integer, Point> {

		@Override
		public Tuple2<Integer, Point> call(Tuple2<Integer, Tuple3<Point, Long, Integer>> t) throws Exception {
		//	System.err.println("spark averager in "+t);
			// t._2()._1().div(t._2()._2());
			Point p = t._2()._1();
			Long l = t._2()._2();
			Point nev = p.div(l);
			Tuple2<Integer, Point> cen = new Tuple2<Integer, Point>(t._1(), nev);
		//	System.err.println("spark centroid ="+cen);
			return cen;
		}
	}


	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	public static class Point implements Serializable {

		private double [] points;

		public Point() { }

		public Point(double[] points) {
			this.points = points.clone();
		}

		public Point add(Point other) {
			Point ret = new Point(this.points);
			for (int i = 0; i < points.length; i++) {
				ret.points[i] =  points[i] + other.points[i];
			}
			return ret;
		}

		public Point div(long val) {
			Point ret = new Point(this.points);
			for (int i = 0; i < points.length; i++) {
				ret.points[i] = points[i] / val;
			}
			return ret;
		}

		public double euclideanDistance(Point other) {
			double sum = 0;
			for (int i = 0; i < points.length; i++) {
				sum = sum + (points[i] - other.points[i]) * (points[i] - other.points[i]);
			}
			return Math.sqrt(sum);
		}

		@Override
		public String toString() {
			return Arrays.toString(points);
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String master = null;
	private static String pointsPath = null;
	private static String centersPath = null;
	private static String outputPath = null;
	private static int numIterations = 10;
	private static int dop = 400;

	private static boolean parseParameters(String[] programArguments) {
		// parse input arguments
		if(programArguments.length == 6) {
			master = programArguments[0];
			pointsPath = programArguments[1];
			centersPath = programArguments[2];
			outputPath = programArguments[3];
			numIterations = Integer.parseInt(programArguments[4]);
			dop = Integer.parseInt(programArguments[5]);
		} else {
			System.err.println("Usage: KMeans <master> <points path> <centers path> <result path> <num iterations> <dop>");
			return false;
		}
		return true;
	}
}