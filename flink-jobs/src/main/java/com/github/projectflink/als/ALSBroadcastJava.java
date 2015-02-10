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

package com.github.projectflink.als;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.github.projectflink.common.als.ALSUtils;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;
import org.jblas.FloatMatrix;
import org.jblas.SimpleBlas;
import org.jblas.Solve;


@SuppressWarnings("serial")
public class ALSBroadcastJava {

	private static final String BC_MATRIX_NAME = "matrix";

	private static final long RANDOM_SEED = 0xDEADBADC0FFEEL;


	public static void main(String[] args) throws Exception {

		final int numLatentFactors;
		final int numIterations;
		final double lambda;

		final String inPath;
		final String outPath;
		final String persistencePath;

		if (args.length < 6) {
			numLatentFactors = 5;
			numIterations = 1;
			lambda = 1.0;
			persistencePath = null;
			inPath = null;
			outPath = null;
		}
		else {
			numLatentFactors = Integer.parseInt(args[0]);
			numIterations = Integer.parseInt(args[2]);
			lambda = Double.parseDouble(args[1]);
			persistencePath = args[3];
			inPath = args[4];
			outPath = args[5];
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		final DataSet<Rating> ratings = (inPath == null) ?
				env.fromElements(ALSSampleData.RATINGS_TUPLES) :
				env.readCsvFile(inPath).tupleType(Rating.class);

		DataSet<Rating> swappedRatings = ratings.map(new MapFunction<Rating, Rating>() {
			@Override
			public Rating map(Rating value) {
				return new Rating(value.getItem(), value.getUser(), value.getRating());
			}
		});

		// group the ratings by Item
		DataSet<Tuple2<Integer, Pair[]>> ratingsPerUser = groupAndCollectAsArray(ratings);

		// group the ratings by Item
		DataSet<Tuple2<Integer, Pair[]>> ratingsPerItem = groupAndCollectAsArray(swappedRatings);

		// use a random ratings matrix
		DataSet<Factors> initialItemMatrix = generateRandomMatrix(
				ratings.<Tuple1<Integer>>project(1).distinct(),
				numLatentFactors, RANDOM_SEED);

		if(persistencePath != null){
			String path;

			if(!persistencePath.endsWith("/")){
				path = persistencePath + "/";
			}else{
				path = persistencePath;
			}

			Path itemMatrixPath = new Path(path + "initialItemMatrix");
			Path ratingsPerUserPath = new Path(path + "userRatings");
			Path ratingsPerItemPath = new Path(path + "itemRatings");

			TypeSerializerOutputFormat<Factors> iiMatrixOF = new
					TypeSerializerOutputFormat<Factors>();
			iiMatrixOF.setOutputFilePath(itemMatrixPath);
			iiMatrixOF.setWriteMode(WriteMode.OVERWRITE);

			initialItemMatrix.output(iiMatrixOF);

			TypeSerializerOutputFormat<Tuple2<Integer, Pair[]>> userRatingsOF = new
					TypeSerializerOutputFormat<Tuple2<Integer, Pair[]>>();
			userRatingsOF.setOutputFilePath(ratingsPerUserPath);
			userRatingsOF.setWriteMode(WriteMode.OVERWRITE);

			ratingsPerUser.output(userRatingsOF);

			TypeSerializerOutputFormat<Tuple2<Integer, Pair[]>> itemRatingsOF = new
					TypeSerializerOutputFormat<Tuple2<Integer, Pair[]>>();
			itemRatingsOF.setOutputFilePath(ratingsPerItemPath);
			itemRatingsOF.setWriteMode(WriteMode.OVERWRITE);

			ratingsPerItem.output(itemRatingsOF);

			env.execute("Preprocessing");

			TypeSerializerInputFormat<Factors> iiMatrixIF = new
					TypeSerializerInputFormat<Factors>(initialItemMatrix.getType());
			iiMatrixIF.setFilePath(itemMatrixPath);

			initialItemMatrix = env.createInput(iiMatrixIF, initialItemMatrix.getType());

			TypeSerializerInputFormat<Tuple2<Integer, Pair[]>> userRatingsIF = new
					TypeSerializerInputFormat<Tuple2<Integer, Pair[]>>(ratingsPerUser.getType());
			userRatingsIF.setFilePath(ratingsPerUserPath);

			ratingsPerUser = env.createInput(userRatingsIF, ratingsPerUser.getType());

			TypeSerializerInputFormat<Tuple2<Integer, Pair[]>> itemRatingsIF = new
					TypeSerializerInputFormat<Tuple2<Integer, Pair[]>>(ratingsPerItem.getType());
			itemRatingsIF.setFilePath(ratingsPerItemPath);

			ratingsPerItem = env.createInput(itemRatingsIF, ratingsPerItem.getType());
		}


		IterativeDataSet<Factors> itemsIteration = initialItemMatrix.iterate(numIterations);

		DataSet<Factors> userMatrix = ratingsPerUser.map(new Solver(numLatentFactors, lambda, BC_MATRIX_NAME)).withBroadcastSet(itemsIteration, BC_MATRIX_NAME);
		DataSet<Factors> itemsMatrix = ratingsPerItem.map(new Solver(numLatentFactors, lambda, BC_MATRIX_NAME)).withBroadcastSet(userMatrix, BC_MATRIX_NAME);

		DataSet<Factors> itemsResult = itemsIteration.closeWith(itemsMatrix);

		if(persistencePath != null){
			String path = persistencePath;

			if(!persistencePath.endsWith("/")){
				path += "/";
			}

			Path itemsResultPath = new Path(path + "itemsMatrix");

			TypeSerializerOutputFormat iOF = new TypeSerializerOutputFormat();
			iOF.setOutputFilePath(itemsResultPath);
			iOF.setWriteMode(WriteMode.OVERWRITE);

			itemsResult.output(iOF);

			env.execute("Post iteration");

			TypeSerializerInputFormat iIF = new TypeSerializerInputFormat(itemsResult.getType());
			iIF.setFilePath(itemsResultPath);

			itemsResult = env.createInput(iIF, itemsResult.getType());
		}

		DataSet<Factors> usersResult = ratingsPerUser.map(new Solver(numLatentFactors, lambda,
				BC_MATRIX_NAME)).withBroadcastSet(itemsResult, BC_MATRIX_NAME);

		if (outPath == null) {
			usersResult.print();
			itemsResult.print();
		}
		else {
			String path = outPath;
			if(!outPath.endsWith("/")){
				path += "/";
			}

			String usersResultOutPath = path + "usersResult";
			String itemsResultOutPath = path + "itemsResult";

			usersResult.writeAsText(usersResultOutPath, WriteMode.OVERWRITE);
			itemsResult.writeAsText(itemsResultOutPath, WriteMode.OVERWRITE);
//			itemsResult.writeAsCsv(outPath, WriteMode.OVERWRITE);
		}

//		System.out.println(env.getExecutionPlan());

		env.execute("ALS Broadcast");
	}


	// --------------------------------------------------------------------------------------------
	//  Utility Methods
	// --------------------------------------------------------------------------------------------

	private static DataSet<Factors> generateRandomMatrix(DataSet<Tuple1<Integer>> ids, final int numFactors, final long seed) {

		return ids.map(new MapFunction<Tuple1<Integer>, Factors>() {

			private final Random rnd = new Random(seed);

			@Override
			public Factors map(Tuple1<Integer> value) {
				float[] vals = new float[numFactors];
				for (int i = 0; i < numFactors; i++) {
					vals[i] = rnd.nextFloat();
				}
				return new Factors(value.f0, vals);
			}
		});
	}

	private static DataSet<Tuple2<Integer, Pair[]>> groupAndCollectAsArray
			(DataSet<Rating> ratings) {
		return ratings.groupBy(0).reduceGroup(new GroupReduceFunction<Rating, Tuple2<Integer,
				Pair[]>>() {

			private final List<Pair> list = new ArrayList<Pair>();
			@Override
			public void reduce(Iterable<Rating> values, Collector<Tuple2<Integer,
					Pair[]>> out) throws Exception {
				int userID = 0;

				for (Rating t : values) {
					userID = t.getUser();
					list.add(new Pair(t.getItem(), t.getRating()));
				}

				out.collect(new Tuple2<Integer, Pair[]>(userID, list.toArray(new Pair[list.size
						()])));
				list.clear();
			}
		});
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Data Types
	// --------------------------------------------------------------------------------------------

	public static final class Rating extends Tuple3<Integer, Integer, Float> {

		public Rating() {}

		public Rating(Integer user, Integer item, Float rating) {
			super(user, item, rating);
		}

		public Integer getUser() {
			return f0;
		}

		public Integer getItem() {
			return f1;
		}

		public Float getRating() {
			return f2;
		}

		public void setUser(Integer value) {
			f0 = value;
		}

		public void setItem(Integer value) {
			f1 = value;
		}

		public void setRating(Float value) {
			f2 = value;
		}
	}

	public static final class Pair extends Tuple2<Integer, Float> {
		public Pair() {}

		public Pair(Integer item, Float rating) {
			super(item, rating);
		}

		public Integer getItem() {
			return f0;
		}

		public Float getRating() {
			return f1;
		}

		public void setItem(Integer value){
			f0 = value;
		}

		public void setRating(Float value){
			f1 = value;
		}
	}

	public static final class Factors extends Tuple2<Integer, float[]> {

		public Factors() {}

		public Factors(Integer id, float[] factors) {
			super(id, factors);
		}

		public Integer getId() {
			return f0;
		}

		public float[] getFactors() {
			return f1;
		}

		@Override
		public String toString(){
			return "(" + f0 + ", " + Arrays.toString(f1) + ")";
		}
	}

	public static class Solver extends RichMapFunction<Tuple2<Integer, Pair[]>, Factors>{
		private final int numFactors;
		private final double lambda;
		private final String bcVarName;
		private final int triangleSize;
		private final FloatMatrix xtx;
		private final FloatMatrix vector;
		private final FloatMatrix fullMatrix;

		private Map<Integer, FloatMatrix> matrix = null;

		public Solver(int numFactors, double lambda, final String bcVarName) {
			this.numFactors = numFactors;
			this.lambda = lambda;
			this.bcVarName = bcVarName;

			triangleSize = (numFactors*numFactors - numFactors)/2 + numFactors;

			xtx = FloatMatrix.zeros(triangleSize);
			vector = FloatMatrix.zeros(numFactors);
			fullMatrix = FloatMatrix.zeros(numFactors, numFactors);
		}

		@Override
		public void open(Configuration parameters){
			matrix = getRuntimeContext().getBroadcastVariableWithInitializer(bcVarName, new
					MatrixBuilder());
		}

		@Override
		public Factors map(Tuple2<Integer, Pair[]> integerTuple2) throws Exception {
			xtx.fill(0.0f);
			vector.fill(0.0f);

			int n = integerTuple2.f1.length;

			for(Pair p: integerTuple2.f1){
				FloatMatrix v = matrix.get(p.f0);

				ALSUtils.outerProductInPlace(v, xtx, numFactors);
				SimpleBlas.axpy(p.f1, v, vector);
			}

			ALSUtils.generateFullMatrix(xtx, fullMatrix, numFactors);

			for(int i =0; i < numFactors; i++){
				fullMatrix.data[i*numFactors + i] += (float)(n * lambda);
			}

			return new Factors(integerTuple2.f0, Solve.solvePositive(fullMatrix, vector).data);
		}
	}

	public static class MatrixBuilder implements BroadcastVariableInitializer<Factors, Map<Integer,
			FloatMatrix>>{

		@Override
		public Map<Integer, FloatMatrix> initializeBroadcastVariable(Iterable<Factors> iterable) {
			Map<Integer, FloatMatrix> matrix = new HashMap<Integer, FloatMatrix>();

			for(Factors factors: iterable){
				matrix.put(factors.getId(), new FloatMatrix(factors.getFactors()));
			}

			return matrix;
		}
	}
}
