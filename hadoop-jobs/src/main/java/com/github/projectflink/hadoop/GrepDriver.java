package com.github.projectflink.hadoop;


import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GrepDriver {


    public static class Grep extends Mapper<LongWritable, Text, Text, Text> {

        private final Text out = new Text();

        private Pattern p;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String pattern = context.getConfiguration().get("pattern");
            Preconditions.checkArgument(pattern != null);
            p = Pattern.compile(pattern);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            if (val == null || val.length() == 0) {
                return;
            }

            final Matcher m = p.matcher(val);
            if (m.find()) {
                out.set(val);
                context.write(out, null);
            }
        }
    }

    public static void main(String [] args) throws Exception {

        String in = args[0];
        String out = args[1];
        System.err.println("Using input=" + in);
        System.err.println("Using output=" + out);

        String patterns[] = new String[args.length - 2];
        System.arraycopy(args, 2, patterns, 0, args.length - 2);
        System.err.println("Using patterns: " + Arrays.toString(patterns));

        for (int i = 0; i < patterns.length; i++) {
            String pattern = patterns[i];
            Configuration conf = new Configuration();
            conf.set("pattern", pattern);
            Job job = Job.getInstance(conf, "Grep for " + pattern);
            job.setMapperClass(Grep.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setNumReduceTasks(0);
            job.setJarByClass(Grep.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1] + "_" + pattern));

            if (!job.waitForCompletion(true)) {
                throw new RuntimeException("Grep job " + i + " failed");
            }
        }
    }

}