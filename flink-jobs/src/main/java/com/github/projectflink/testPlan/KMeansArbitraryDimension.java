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
package com.github.projectflink.testPlan;

import java.io.Serializable;
import java.util.Collection;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.StringUtils;

/**
 * This example implements a basic K-Means clustering algorithm.
 *
 * <p>
 * K-Means is an iterative clustering algorithm and works as follows:<br>
 * K-Means is given a set of data points to be clustered and an initial set of <i>K</i> cluster centers.
 * In each iteration, the algorithm computes the distance of each data point to each cluster center.
 * Each point is assigned to the cluster center which is closest to it.
 * Subsequently, each cluster center is moved to the center (<i>mean</i>) of all points that have been assigned to it.
 * The moved cluster centers are fed into the next iteration. 
 * The algorithm terminates after a fixed number of iterations (as in this implementation) 
 * or if cluster centers do not (significantly) move in an iteration.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/K-means_clustering">K-Means Clustering algorithm</a>.
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Data points are represented as double values separated by a blank character.
 * Data points are separated by newline characters.<br>
 * For example <code>"1.2 2.3\n5.3 7.2\n"</code> gives two data points (x=1.2, y=2.3) and (x=5.3, y=7.2).
 * <li>Cluster centers are represented by an integer id and a point value.<br>
 * For example <code>"1 6.2 3.2\n2 2.9 5.7\n"</code> gives two centers (id=1, x=6.2, y=3.2) and (id=2, x=2.9, y=5.7).
 * </ul>
 *
 * <p>
 * Usage: <code>KMeans &lt;points path&gt; &lt;centers path&gt; &lt;result path&gt; &lt;num iterations&gt;</code><br>
 *
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Bulk iterations
 * <li>Broadcast variables in bulk iterations
 * <li>Custom Java objects (PoJos)
 * </ul>
 */
@SuppressWarnings("serial")
public class KMeansArbitraryDimension {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Point> points = env
			.readTextFile(pointsPath)
			.map(new ConvertToPoint());


		DataSet<Centroid> centroids = env
			.readTextFile(centersPath)
			.map(new ConvertToCentroid());


		// set number of bulk iterations for KMeans algorithm
		IterativeDataSet<Centroid> loop = centroids.iterate(numIterations);

		DataSet<Centroid> newCentroids = points
			// compute closest centroid for each point
			.map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
			// count and sum point coordinates for each centroid
			.map(new CountAppender())
			.groupBy(0).reduce(new CentroidAccumulator())
			// compute new centroids from point counts and coordinate sums
			.map(new CentroidAverager());
		// feed new centroids back into next iteration
		DataSet<Centroid> finalCentroids = loop.closeWith(newCentroids);

		DataSet<Tuple2<Integer, Point>> clusteredPoints = points
			// assign points to final clusters
			.map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");

		// emit result
		//clusteredPoints.writeAsCsv(outputPath, "\n", " ", FileSystem.WriteMode.OVERWRITE);
		clusteredPoints.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);
		// execute program
		env.setParallelism(dop);
		env.execute("KMeans Multi-Dimension");

	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	public static class Point implements Serializable {

		public double [] points;

		public Point() {}

		public Point(double [] points) {
			this.points = points;
		}

		public Point add(Point other) {
			for (int i = 0; i < points.length; i++) {
				points[i] =  points[i] + other.points[i];
			}
			return this;
		}

		public Point div(long val) {
			for (int i = 0; i < points.length; i++) {
				points[i] = points[i] / val;
			}
			return this;
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
			return StringUtils.arrayToString(points);
		}
	}

	public static class Centroid extends Point {

		public int id;

		public Centroid() {}

		public Centroid(int id, double [] points) {
			super(points);
			this.id = id;
		}

		public Centroid(int id, Point p) {
			super(p.points);
			this.id = id;
		}

		@Override
		public String toString() {
			return id + "," + super.toString();
		}
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************


	/** Convert String value into data point **/
	public static final class ConvertToPoint implements MapFunction<String, Point> {

		@Override
		public Point map(String s) throws Exception {
			String [] line = s.split(" ");
			double [] points = new double[line.length];
			for (int i = 0; i < line.length; i++) {
				points[i] = Double.parseDouble(line[i]);
			}
			return new Point(points);
		}
	}

	/** Convert String value into data centroid **/
	public static final class ConvertToCentroid implements MapFunction<String, Centroid> {

		@Override
		public Centroid map(String s) throws Exception {
			String [] line = s.split(" ");
			int id = Integer.parseInt(line[0]);
			double [] points = new double[line.length - 1];
			for (int i = 1; i < line.length; i++) {
				points[i - 1] = Double.parseDouble(line[i]);
			}
			return new Centroid(id, points);
		}
	}

	/** Determines the closest cluster center for a data point. */
	public static final class SelectNearestCenter extends RichMapFunction<Point, Tuple2<Integer, Point>> {
		private Collection<Centroid> centroids;

		/** Reads the centroid values from a broadcast variable into a collection. */
		@Override
		public void open(Configuration parameters) throws Exception {
			this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
		}

		@Override
		public Tuple2<Integer, Point> map(Point p) throws Exception {

			double minDistance = Double.MAX_VALUE;
			int closestCentroidId = -1;

			// check all cluster centers
			for (Centroid centroid : centroids) {
				// compute distance
				double distance = p.euclideanDistance(centroid);

				// update nearest cluster if necessary 
				if (distance < minDistance) {
					minDistance = distance;
					closestCentroidId = centroid.id;
				}
			}

			// emit a new record with the center id and the data point.
			return new Tuple2<Integer, Point>(closestCentroidId, p);
		}
	}

	/** Appends a count variable to the tuple. */
	public static final class CountAppender implements MapFunction<Tuple2<Integer, Point>, Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> map(Tuple2<Integer, Point> t) {
			Tuple3<Integer, Point, Long> r = new Tuple3<Integer, Point, Long>(t.f0, t.f1, 1L);
			return r;
		}
	}

	/** Sums and counts point coordinates. */
	public static final class CentroidAccumulator implements ReduceFunction<Tuple3<Integer, Point, Long>> {

		@Override
		public Tuple3<Integer, Point, Long> reduce(Tuple3<Integer, Point, Long> val1, Tuple3<Integer, Point, Long> val2) {
			Tuple3<Integer, Point, Long> r = new Tuple3<Integer, Point, Long>(val1.f0, val1.f1.add(val2.f1), val1.f2 + val2.f2);
			return r;
		}
	}

	/** Computes new centroid from coordinate sum and count of points. */
	public static final class CentroidAverager implements MapFunction<Tuple3<Integer, Point, Long>, Centroid> {

		@Override
		public Centroid map(Tuple3<Integer, Point, Long> value) {
			Centroid c= new Centroid(value.f0, value.f1.div(value.f2));
			return c;
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String pointsPath = null;
	private static String centersPath = null;
	private static String outputPath = null;
	private static int numIterations = 10;
	private static int dop;

	private static boolean parseParameters(String[] programArguments) {
		// parse input arguments
		if(programArguments.length == 5) {
			pointsPath = programArguments[0];
			centersPath = programArguments[1];
			outputPath = programArguments[2];
			numIterations = Integer.parseInt(programArguments[3]);
			dop = Integer.parseInt(programArguments[4]);
		} else {
			System.err.println("Usage: KMeans <points path> <centers path> <result path> <num iterations>");
			return false;
		}
		return true;
	}

}
