package com.cloudera.sa.ArcmainMapper;

import java.io.*;

import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.cloudera.sa.ArcmainMapper.util.FixedLengthInputFormat;


public class FileSplitCompressorJob {


    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out
                    .println("FileCompressorJob <inputPath> <outputPath>");
            System.out.println();
            System.out
                    .println("Example: FileCompressorJob ./input ./output");
            return;
        }

        // Get values from args
        String inputPath = args[0];
        String outputPath = args[1];


        Configuration conf = new Configuration();
        conf.set("outputPath", outputPath);
        conf.set("mapred.map.tasks.speculative.execution", "false");
        conf.set("mapred-D mapred.task.timeout","0");

        // Create job
        Job job = new Job(conf);
        job.setJobName("FileSplitCompressorJob");


        job.setJarByClass(FileSplitCompressorJob.class);
        // Define input format and path
        job.setInputFormatClass(FixedLengthInputFormat.class);
        FixedLengthInputFormat.setRecordLength(job, 20000);
        FixedLengthInputFormat.addInputPath(job, new Path(inputPath));

        // Define output format
        job.setOutputFormatClass(NullOutputFormat.class);
        // Define the mapper and reducer
        job.setMapperClass(compressSplitMapper.class);
        // job.setReducerClass(Reducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
    }

    public static class compressSplitMapper extends
            Mapper<LongWritable, BytesWritable, NullWritable, NullWritable> {
        String outputDir;
        String compressionCodec;
        Configuration config;
        FileSystem hdfs;
        int buffer;
        Path targetFilePath = null;
        GZIPOutputStream fileOut = null;
        String filename;


        @Override
        public void setup(Context context) throws IOException {
            //config = new Configuration();
            config = context.getConfiguration();
            hdfs = FileSystem.get(config);
            outputDir = config.get("outputPath");

            InputSplit is = context.getInputSplit();
            String origName = is.toString();
            System.out.println("The input split is named " + origName);

            int i = origName.lastIndexOf('/');
            //int j = origName.lastIndexOf('.');
            int k = origName.lastIndexOf(':');
            int l = origName.lastIndexOf('+');
            filename = origName.substring(i+1, k);
            String startByte = origName.substring(k + 1, l);


            System.out.println("The input split " + is);
            System.out.println("The Filename is " + filename);

            targetFilePath = new Path(outputDir + "/" + filename + "-part-m-" + startByte + "-" + StringUtils.leftPad(
                    Integer.toString(context.getTaskAttemptID()
                            .getTaskID().getId()), 5, '0')+ ".gz");
            fileOut = new GZIPOutputStream(hdfs.create(targetFilePath));

        }

        @Override
        public void cleanup(Context context) throws IOException {
            fileOut.close();
        }
        long lastLength = 0;

        @Override
        public void map(LongWritable key, BytesWritable value, Context context)  throws IOException, InterruptedException{
            //System.out.println("Mapper is about to process: " + value.toString());

            byte[] bytes = value.getBytes();
            int len = Integer.parseInt(key.toString());
            if (len != lastLength) {
                byte[] shortRecord = new byte[len];
                System.arraycopy(bytes, 0, shortRecord, 0, len);
                fileOut.write(shortRecord);
            } else {
                fileOut.write(bytes);
            }
        }

    }

}
