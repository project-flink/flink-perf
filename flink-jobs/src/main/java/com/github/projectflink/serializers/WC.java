package com.github.projectflink.serializers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * Created by robert on 2/18/15.
 */
public class WC {

	/**
	 * This is the POJO (Plain Old Java Object) that is being used
	 * for all the operations.
	 * As long as all fields are public or have a getter/setter, the system can handle them
	 */
	public static class Word {
		// fields
		private String word;
		private Integer frequency;

		// constructors
		public Word() {
		}
		public Word(String word, int i) {
			this.word = word;
			this.frequency = i;
		}
		// getters setters
		public String getWord() {
			return word;
		}
		public void setWord(String word) {
			this.word = word;
		}
		public Integer getFrequency() {
			return frequency;
		}
		public void setFrequency(Integer frequency) {
			this.frequency = frequency;
		}
		// to String
		@Override
		public String toString() {
			return "Word="+word+" freq="+frequency;
		}
	}

	public static void main(String[] args) throws Exception {
		textPath = args[0];
		outputPath = args[1];
		String mode = args[2];

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		if(mode.equals("forcekryo")) {
			env.getConfig().enableForceKryo();
		} else if(mode.equals("forceavro")) {
			env.getConfig().enableForceKryo();
			env.getConfig().enableGenericTypeSerializationWithAvro();
		}

		// get input data
		DataSet<String> text = env.readTextFile(textPath);

		DataSet<Word> tokens =
				// split up the lines into Word objects (with frequency = 1)
				text.flatMap(new Tokenizer());
						// group by the field word and sum up the frequency
						//
		UnsortedGrouping<Word> o = null;
		if(mode.equals("pojo")) {
			o = tokens.groupBy("word");
		} else if(mode.equals("forcekryo") || mode.equals("forceavro")) {
			o = tokens.groupBy(new KeySelector<Word, String>() {
				@Override
				public String getKey(Word value) throws Exception {
					return value.getWord();
				}
			});
		} else {
			throw new RuntimeException("Wrong mode "+mode);
		}

		DataSet<Word> counts = o.reduce(new ReduceFunction<Word>() {
			@Override
			public Word reduce(Word value1, Word value2) throws Exception {
				return new Word(value1.word, value1.frequency + value2.frequency);
			}
		});

		counts.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("WordCount-Pojo Example in mode "+mode);
	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined
	 * FlatMapFunction. The function takes a line (String) and splits it into
	 * multiple pairs in the form of "(word,1)" (Tuple2<String, Integer>).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Word> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Word> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Word(token, 1));
				}
			}
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static String textPath;
	private static String outputPath;

}
