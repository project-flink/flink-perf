package com.github.projectflink.hadoop;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class PageRankDriver {

	private static final Log LOG = LogFactory.getLog(PageRankDriver.class);

	private static final double DAMPENING_FACTOR = 0.85;
	private static double RANDOM_JUMP;

	public static class Message implements Writable {

		public double prob;
		public long [] neighbors;

		public Message() {
		}

		public Message(double prob, long[] neighbors) {
			this.prob = prob;
			this.neighbors = neighbors;
		}

		public Message(double prob) {
			this.prob = prob;
			this.neighbors = null;
		}

		public int numNeighbors () {
			return neighbors.length;
		}

		@Override
		public void write(DataOutput dataOutput) throws IOException {
			dataOutput.writeDouble(prob);
			if (neighbors == null) {
				dataOutput.writeBoolean(false);
			}
			else {
				dataOutput.writeBoolean(true);
				dataOutput.writeInt(neighbors.length);
				for (int i = 0; i < neighbors.length; i++) {
					dataOutput.writeLong(neighbors[i]);
				}
			}
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			this.prob = dataInput.readDouble();
			boolean hasNeighbors = dataInput.readBoolean();
			if (hasNeighbors) {
				int l = dataInput.readInt();
				this.neighbors = new long[l];
				for (int i = 0; i < l; i++) {
					this.neighbors[i] = dataInput.readLong();
				}
			}
			else {
				this.neighbors = null;
			}
		}
	}


	public static class PageRankMapper extends Mapper<LongWritable, Message, LongWritable, Message> {

		private Message m = new Message(-1.0, null);
		private LongWritable nid = new LongWritable();
		private double randomJump;
		private double dampeningFactor;


		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			randomJump = Double.parseDouble(context.getConfiguration().get("random_jump"));
			dampeningFactor = Double.parseDouble(context.getConfiguration().get("dampening_factor"));
		}

		@Override
		protected void map(LongWritable key, Message value, Context context) throws IOException, InterruptedException {
			int n = value.numNeighbors();
			double p = value.prob / n;
			context.write (key, value);
			for (int i = 0; i < n; i++) {
				nid.set(value.neighbors[i]);
				m.prob = p  * dampeningFactor;
				context.write(nid, m);
			}
			m.prob = (1.0 - dampeningFactor) * randomJump;
			context.write (key, m);
		}
	}

	public static class PageRankReducer extends Reducer<LongWritable, Message, LongWritable, Message> {

		private Message out = new Message();

		@Override
		protected void reduce(LongWritable key, Iterable<Message> values, Context context) throws IOException, InterruptedException {

			double rank = 0.0;
			for (Message m : values) {
				if (m.neighbors != null) {
					out.neighbors = Arrays.copyOf(m.neighbors, m.neighbors.length);
				}
				else {
					rank += m.prob;
				}
			}
			out.prob = rank;
			context.write(key, out);
		}
	}

	public static class InitialRankAssigner extends Mapper<LongWritable, Text, LongWritable, Message> {

		private double randomJump;
		private Message outValue = new Message();
		private LongWritable outKey = new LongWritable();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			randomJump = Double.parseDouble(context.getConfiguration().get("random_jump"));
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, " ");
			int count = tokenizer.countTokens();
			//LOG.info("Number of tokens for line " + line + " is " + count);
			outKey.set(Long.valueOf(tokenizer.nextToken()));
			long [] neighbors = new long[count-1];
			for (int i = 0; i < neighbors.length; i++) {
				neighbors[i] = Long.valueOf(tokenizer.nextToken());
			}
			outValue.neighbors = neighbors;
			outValue.prob = randomJump;
			context.write(outKey, outValue);
		}
	}

	public static class RankPrinter extends Mapper<LongWritable, Message, Text, Text> {

		Text text = new Text();
		Text empty = new Text();

		@Override
		protected void map(LongWritable key, Message value, Context context) throws IOException, InterruptedException {
			String out = key.get() + " " + value.prob;
			text.set(out);
			context.write(text, empty);
		}
	}


	public static void assignInitialRanks (Configuration conf, FileSystem fs, String adjacencyPath, String initialPath, int numVertices) throws Exception {
		Path seqFile = new Path (initialPath);
		if (fs.exists(seqFile)) {
			fs.delete(seqFile, true);
		}
		Job job = Job.getInstance(conf);
		job.setMapperClass(InitialRankAssigner.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Message.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Message.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(adjacencyPath));
		FileOutputFormat.setOutputPath(job, seqFile);
		job.waitForCompletion(true);
	}

	public static void calculateNextRanks (Configuration conf, FileSystem fs, String inputPath, String outputPath) throws Exception {
		Path outFile = new Path (outputPath);
		if (fs.exists(outFile)) {
			fs.delete(outFile, true);
		}
		Job job = Job.getInstance(conf);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Message.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Message.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, outFile);
		job.waitForCompletion(true);
	}

	public static void printFinalRanks (Configuration conf, FileSystem fs, String inputPath, String outputPath) throws Exception {
		Path outFile = new Path (outputPath);
		if (fs.exists(outFile)) {
			fs.delete(outFile, true);
		}
		Job job = Job.getInstance(conf);
		job.setMapperClass(RankPrinter.class);
		job.setReducerClass(Reducer.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, outFile);
		job.waitForCompletion(true);
	}


	public static void main (String [] args) throws Exception {

		String adjacencyFile = args[0];
		String resultFile = args[1];
		int numVertices = Integer.valueOf(args[2]);
		int numIterations = Integer.valueOf(args[3]);


		Configuration conf = new Configuration();
		RANDOM_JUMP = 1.0 / ((double) numVertices);
		conf.set("random_jump", String.valueOf(RANDOM_JUMP));
		conf.set("dampening_factor", String.valueOf(DAMPENING_FACTOR));

		FileSystem fs = FileSystem.get(conf);
		String adjacencySeq = adjacencyFile + "_seq";

		assignInitialRanks(conf, fs, adjacencyFile, adjacencySeq, numVertices);



		String inputFile = adjacencySeq;
		String outputFile = null;

		for (int iteration = 0; iteration < numIterations; iteration++) {
			outputFile = "/pageranks_iteration_" + iteration;
			calculateNextRanks(conf, fs, inputFile, outputFile);
			inputFile = outputFile;
		}




		printFinalRanks(conf, fs, outputFile, resultFile);
	}
}


