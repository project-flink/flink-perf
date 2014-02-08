package eu.stratosphere.test.manyFunctions;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


/**
 * Very simple generator to create a (large) sequencefile to test in
 * conjunction with the large Stratosphere test job.
 * 
 * Based on http://stackoverflow.com/questions/5377118/how-to-convert-txt-file-to-hadoops-sequence-file-format
 *
 */
public class SequenceFileGenerator {

	public SequenceFileGenerator() {
	}
	
	public static void main(String[] args) throws IOException {
		if(args.length < 3) {
			System.err.println("Usage: <outFilePath> <KV Count> <String Length>");
			System.exit(1);
		}
		String uri = args[0];
		int kvCount = Integer.parseInt(args[1]);
		int strlen = Integer.parseInt(args[2]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create( uri), conf);
        Path path = new Path(uri);
        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try { 
            writer = SequenceFile.createWriter( fs, conf, path, key.getClass(), value.getClass());
            for (int i = 0; i < kvCount; i ++) { 
                key.set(i);
                value.set(RandomStringUtils.randomNumeric(strlen));
                writer.append( key, value); } 
        } finally 
        { IOUtils.closeStream( writer); 
        } 
	}

}
