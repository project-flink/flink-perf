package com.github.projectflink.generators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Random;

/**
 * Generates texts.
 */
public class Text {
    private static String[] sentenceEnds = {".", "...", "?", "??", "!", "-- "};

    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        int dop = Integer.valueOf(args[0]);
        String outPath = args[1];
        long finalSizeGB = Integer.valueOf(args[2]);
        final long bytesPerMapper = ((finalSizeGB * 1024 * 1024 * 1024) / dop);
        System.err.println("Generating Text data with the following properties:\n"
                + "dop="+dop+" outPath="+outPath+" finalSizeGB="+finalSizeGB+" bytesPerMapper="+bytesPerMapper);

        DataSet<Long> empty = env.generateSequence(1, dop);
        DataSet<String> logLine = empty.flatMap(new FlatMapFunction<Long, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void flatMap(Long value, Collector<String> out) throws Exception {
                Random rnd = new Utils.XORShiftRandom();
                StringBuffer sb = new StringBuffer();
                long bytesGenerated = 0;
                while(true) {
                    int sentenceLength = rnd.nextInt(25); // up to 16 words per sentence
                    for(int i = 0; i < sentenceLength; i++) {
                        sb.append(Utils.getFastZipfRandomWord());
                        sb.append(' ');
                    }
                    sb.append(sentenceEnds[rnd.nextInt(sentenceEnds.length-1)]);
                    final String str = sb.toString();
                    sb.delete(0, sb.length());
                    bytesGenerated += str.length();
                    out.collect(str);
                    // System.err.println("line ="+str);
                    if(bytesGenerated > bytesPerMapper) {
                        break;
                    }
                }
            }
        }).setParallelism(dop);
        logLine.writeAsText(outPath, FileSystem.WriteMode.OVERWRITE);
        env.setDegreeOfParallelism(dop);
        env.execute("Flink Distributed Text Data Generator");
    }
}
