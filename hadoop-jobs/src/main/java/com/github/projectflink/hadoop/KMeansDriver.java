package com.github.projectflink.hadoop;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/*
 * Code adapted from https://github.com/thomasjungblut/thomasjungblut-common/
 */
public class KMeansDriver implements Tool {

	private static final Log LOG = LogFactory.getLog(KMeansDriver.class);

	public static String CENTERS_CONF_KEY = "centroids.path";

	private Configuration conf;
	@Override
	public void setConf(Configuration configuration) {
		this.conf = configuration;
	}

	@Override
	public Configuration getConf() {
		if(this.conf == null) {
			this.conf = new Configuration();
		}
		return this.conf;
	}

	public static class Point implements Writable{
		double [] coord;
		int n;

		public Point() {
			this.n = 0;
			this.n = 0;
			this.coord = null;
		}

		public Point(double[] coord) {
			this.coord = coord;
			this.n = coord.length;
		}

		public Point (Point p) {
			this.coord = p.coord;
			this.n = p.n;
		}

		public Point add (Point other) {
			for (int i = 0; i < n; i++)
				coord[i] += other.coord[i];
			return this;
		}

		public Point div (double d) {
			for (int i = 0; i < n; i++)
				coord[i] /= d;
			return this;
		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			dataOutput.writeInt(n);
			for (int i = 0; i < n; i++)
				dataOutput.writeDouble(coord[i]);
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			this.n = dataInput.readInt();
			this.coord = new double[n];
			for (int i = 0; i < n; i++)
				this.coord[i] = dataInput.readDouble();
		}


		public double euclideanDistance (Point other) {
			double sumOfSquares = 0.0;
			for (int i = 0; i < n; i++) {
				sumOfSquares += (coord[i] - other.coord[i]) * (coord[i] - other.coord[i]);
			}
			return Math.sqrt(sumOfSquares);
		}

		public Point deepCopy () {
			Point out = new Point ();
			out.n = this.n;
			out.coord = new double[out.n];
			for (int i = 0; i < out.n; i++) {
				out.coord[i] = this.coord[i];
			}
			return out;
		}

		@Override
		public String toString() {
			//String out = "[";
			String out = "";
			for (int i = 0; i < n; i++) {
				out += Double.valueOf(coord[i]);
				if (i < n-1) {
					out += " ";
				}
			}
			return out;
		}
	}

	public static class Centroid extends Point implements WritableComparable<Centroid>{

		private int id;

		public Centroid() {
		}

		public Centroid(Point p) {
			super(p);
			this.id = -1;
		}

		public Centroid (Centroid c) {
			super(c);
			this.id = c.id;
		}

		public Centroid (int id, Point p) {
			super(p);
			this.id = id;
		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			super.write(dataOutput);
			dataOutput.writeInt(id);
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			super.readFields(dataInput);
			this.id = dataInput.readInt();
		}

		@Override
		public String toString() {
			return Integer.toString(id);
		}

		@Override
		public int compareTo(Centroid o) {
			return Integer.compare(this.id, o.id);
		}
	}


	public static class KMeansMapper extends Mapper<Centroid, Point, Centroid, Point> {

		private final List<Centroid> centers = new ArrayList<Centroid>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path centroids = new Path(conf.get(CENTERS_CONF_KEY));
			FileSystem fs = FileSystem.get(conf);

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);

			Centroid key = new Centroid();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				Centroid clusterCenter = new Centroid(key);
				centers.add(clusterCenter);
			}
			//LOG.info("Centroid list in Mapper: " + centers.toString());
			reader.close();
		}

		@Override
		protected void map(Centroid key, Point value, Context context) throws IOException, InterruptedException {
			Centroid nearest = null;
			double nearestDistance = Double.MAX_VALUE;
			for (Centroid c : centers) {
				double dist = value.euclideanDistance(c);
				if (nearest == null) {
					nearest = c;
					nearestDistance = dist;
				}
				else {
					if (dist < nearestDistance) {
						nearest = c;
						nearestDistance = dist;
					}
				}
			}
			context.write(nearest, value);
		}
	}


	public static class KMeansCombiner extends Reducer<Centroid, Point, Centroid, Point> {

		@Override
		protected void reduce(Centroid key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
			ArrayList<Point> points = new ArrayList<Point>();
			points.clear();
			int clusterId = key.id;
			Point newCenter = null;
			for (Point p : values) {
				Point copy = p.deepCopy();
				points.add(copy);
				if (newCenter == null) {
					newCenter = new Point (copy.deepCopy());
				}
				else {
					newCenter = newCenter.add (copy);
				}
			}
			Centroid center = new Centroid(clusterId, newCenter);
			for (Point p: points) {
				context.write(center, p);
			}
		}

	}

	public static class KMeansReducer extends KMeansCombiner {

		final List<Centroid> centers = new ArrayList<Centroid>();

		@Override
		protected void reduce(Centroid key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
			ArrayList<Point> points = new ArrayList<Point>();
			points.clear();
			int clusterId = key.id;
			Point newCenter = null;
			for (Point p : values) {
				Point copy = p.deepCopy();
				points.add(copy);
				if (newCenter == null) {
					newCenter = new Point(copy.deepCopy());
				} else {
					newCenter = newCenter.add(copy);
				}
			}
			newCenter = newCenter.div(points.size());
			Centroid center = new Centroid(clusterId, newCenter);
			centers.add(center);
			for (Point p : points) {
				context.write(center, p);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			Configuration conf = context.getConfiguration();
			Path outPath = new Path(conf.get(CENTERS_CONF_KEY));
			FileSystem fs = FileSystem.get(conf);
			// fs.delete(outPath, true);
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, context.getConfiguration(), outPath, Centroid.class, IntWritable.class);
			final IntWritable mockValue = new IntWritable(0);
			for (Centroid center : centers) {
				writer.append(center, mockValue);
			}
			writer.close();
		}
	}

	public static class CenterInitializer extends Mapper<LongWritable,Text,Centroid,Point> {

		private final List<Centroid> centers = new ArrayList<Centroid>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path centroids = new Path(conf.get(CENTERS_CONF_KEY));
			FileSystem fs = FileSystem.get(conf);

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);

			Centroid key = new Centroid();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				Centroid clusterCenter = new Centroid(key);
				centers.add(clusterCenter);
			}
			reader.close();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int dim = tokenizer.countTokens();
			double [] coords = new double[dim];
			for (int i = 0; i < dim; i++) {
				coords[i] = Double.valueOf(tokenizer.nextToken());
			}
			Point point = new Point (coords);

			Centroid nearest = null;
			double nearestDistance = Double.MAX_VALUE;
			for (Centroid c : centers) {
				double dist = point.euclideanDistance(c);
				if (nearest == null) {
					nearest = c;
					nearestDistance = dist;
				}
				else {
					if (dist < nearestDistance) {
						nearest = c;
						nearestDistance = dist;
					}
				}
			}
			context.write(nearest, point);
		}
	}

	public static class RandomCenterInitializer extends Mapper<LongWritable,Text,Centroid,Point> {

		private final List<Centroid> centers = new ArrayList<Centroid>();
		private final Random rand = new Random();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path centroids = new Path(conf.get(CENTERS_CONF_KEY));
			FileSystem fs = FileSystem.get(conf);

			SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroids, conf);

			Centroid key = new Centroid();
			IntWritable value = new IntWritable();
			while (reader.next(key, value)) {
				Centroid clusterCenter = new Centroid(key);
				centers.add(clusterCenter);
			}
			reader.close();
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int dim = tokenizer.countTokens();
			double [] coords = new double[dim];
			for (int i = 0; i < dim; i++) {
				coords[i] = Double.valueOf(tokenizer.nextToken());
			}
			Centroid center = centers.get(rand.nextInt(centers.size()));
			Point point = new Point (coords);
			context.write(center, point);
		}
	}

	public static class PointSequenceToTextConverter extends Mapper<Centroid, Point, Text, Text> {
		@Override
		protected void map(Centroid key, Point value, Context context) throws IOException, InterruptedException {
			String out = key.toString() + " " + value.toString();
			context.write(new Text(out), new Text(""));
		}
	}

	public static class CenterSequenceToTextConverter extends Mapper<Centroid, IntWritable, Text, Text> {
		@Override
		protected void map(Centroid key, IntWritable value, Context context) throws IOException, InterruptedException {
			context.write(new Text(key.toString()), new Text(value.toString()));
		}
	}

	public static void convertPointsSequenceFileToText (Configuration conf, FileSystem fs, String seqFilePath, String outputPath) throws Exception {

		Path seqFile = new Path (seqFilePath);
		Path output = new Path (outputPath);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		Job job = Job.getInstance(conf);
		job.setMapperClass(PointSequenceToTextConverter.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, seqFile);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
	}

	public static void convertCentersSequenceFileToText (Configuration conf, FileSystem fs, String seqFilePath, String outputPath) throws Exception {

		Path seqFile = new Path (seqFilePath);
		Path output = new Path (outputPath);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		Job job = Job.getInstance(conf);
		job.setMapperClass(CenterSequenceToTextConverter.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, seqFile);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);
	}

	public static void createCentersSequenceFile (Configuration conf, FileSystem fs, String centroidsPath, String sequenceFilePath) throws Exception {
		Path seqFile = new Path (sequenceFilePath);
		if (fs.exists(seqFile)) {
			fs.delete(seqFile, true);
		}
		FSDataInputStream inputStream = fs.open(new Path(centroidsPath));
		SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, seqFile, Centroid.class, IntWritable.class);
		IntWritable value = new IntWritable(0);
		while (inputStream.available() > 0) {
			String line = inputStream.readLine();
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int dim = tokenizer.countTokens() - 1;
			int clusterId = Integer.valueOf(tokenizer.nextToken());
			double [] coords = new double [dim];
			for (int i = 0; i < dim; i++) {
				coords[i] = Double.valueOf(tokenizer.nextToken());
			}
			Centroid cluster = new Centroid(clusterId, new Point(coords));
			writer.append(cluster, value);
		}
		IOUtils.closeStream(writer);
		inputStream.close();
	}


	public static void initializeCenters (Configuration conf, FileSystem fs, String pointsPath, String seqFilePath) throws Exception {
		Path points = new Path (pointsPath);
		Path seqFile = new Path (seqFilePath);
		if (fs.exists(seqFile)) {
			fs.delete(seqFile, true);
		}
		Job job = Job.getInstance(conf);
		job.setMapperClass(CenterInitializer.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Centroid.class);
		job.setMapOutputValueClass(Point.class);
		job.setOutputKeyClass(Centroid.class);
		job.setOutputValueClass(Point.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(pointsPath));
		FileOutputFormat.setOutputPath(job, seqFile);
		job.waitForCompletion(true);
	}

	public static void kmeans (Configuration conf, FileSystem fs, String pointsPath, String resultsPath, int maxIterations) throws Exception {
		Path points = new Path (pointsPath);

		Job job = null;
		Path inPath = null;
		Path outPath = null;
		for (int iteration = 0; iteration < maxIterations; iteration++) {
			job = Job.getInstance(conf);

			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);
			job.setCombinerClass(KMeansReducer.class); // TODO: think about this,
			job.setMapOutputKeyClass(Centroid.class);
			job.setMapOutputValueClass(Point.class);
			job.setOutputKeyClass(Centroid.class);
			job.setOutputValueClass(Point.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.setInputFormatClass(SequenceFileInputFormat.class);

			if (iteration == 0) {
				inPath = points;
			}
			else {
				inPath = new Path(resultsPath + "_iteration_" + (iteration - 1));
			}
			if (iteration == maxIterations - 1) {
				outPath = new Path(resultsPath);
			}
			else {
				outPath = new Path(resultsPath + "_iteration_" + iteration);
			}

			if (fs.exists(outPath)) {
				fs.delete(outPath, true);
			}

			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);

			fs.delete(new Path(conf.get(CENTERS_CONF_KEY)), true);

			if (!job.waitForCompletion(true)) {
				throw new RuntimeException("K-Means iteration " + iteration + " failed");
			}
		}
	}

	@Override
	public int run(String [] args) throws Exception {
		if(this.conf == null) {
			this.conf = new Configuration();
		}

		String points = args[0];
		String centers = args[1];
		String result = args[2];
		int maxIterations = Integer.valueOf(args[3]);
		String centersSeqFile = centers + "_seq";
		String pointsSeqFile = points + "_seq";

		conf.set(CENTERS_CONF_KEY, centersSeqFile);

		FileSystem fs = FileSystem.get(conf);

		createCentersSequenceFile(conf, fs, centers, centersSeqFile);

		initializeCenters(conf, fs, points, pointsSeqFile);

		kmeans (conf, fs, pointsSeqFile, result, maxIterations);

		convertPointsSequenceFileToText(conf, fs, result, result + "_text");

		fs.close();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		KMeansDriver drv = new KMeansDriver();
		drv.getConf().set("mapreduce.framework.name", "local");
		int exitCode = ToolRunner.run(drv, args);
		System.exit(exitCode);
	}
}