package com.cloudera.sa.ArcmainMapper;

import java.io.*;

import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.commons.compress.compressors.bzip2.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.cloudera.sa.ArcmainMapper.util.ConfigurableInputFormat;


public class FileCompressorJob {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out
                    .println("FileCompressorJob <inputPath> <outputPath> <# mappers> <compressionCodec>");
            System.out.println();
            System.out
                    .println("Example: FileCompressorJob ./input ./output 20 gizp");
            return;
        }

        // Get values from args
        String inputPath = args[0];
        String outputPath = args[1];
        String numberOfMappers = args[2];
        String compressionCodec = args[3];

        String buffersize = "";

        if (args.length == 5) {
             buffersize = args[4];
        } else {
            buffersize = "8192";
        }

        //int buffersize = Integer.parseInt(args[4]);

        if ( !((compressionCodec.toLowerCase().equals("gzip")) || (compressionCodec.toLowerCase().equals("bzip2"))) )
        {
            System.out.println("Valid compression codecs are gzip and bzip2");
            return;
        }


        Configuration conf = new Configuration();
        conf.set("compressionCodec", compressionCodec);
        conf.set("outputPath", outputPath);
        conf.set("buffersize", buffersize);

        // Create job
        Job job = new Job(conf);
        job.setJobName("FileCompressorJob");


        job.setJarByClass(FileCompressorJob.class);
        // Define input format and path
        job.setInputFormatClass(ConfigurableInputFormat.class);
        ConfigurableInputFormat.setInputPath(job, inputPath);
        ConfigurableInputFormat.setMapperNumber(job, Integer.parseInt(numberOfMappers));

        // Define output format
        job.setOutputFormatClass(NullOutputFormat.class);
        // Define the mapper and reducer
        job.setMapperClass(compressMapper.class);
        // job.setReducerClass(Reducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        Configuration config = new Configuration();
        //FileSystem hdfs = FileSystem.get(config);

      /// Do we really want this here???
     //   hdfs.delete(new Path(outputPath), true);

        // Exit
        job.waitForCompletion(true);
    }

    public static class compressMapper extends
            Mapper<LongWritable, Text, NullWritable, NullWritable> {
        String outputDir;
        String compressionCodec;
        Configuration config;
        FileSystem hdfs;
        int buffer;


        @Override
        public void setup(Context context) throws IOException {
            //config = new Configuration();
            config = context.getConfiguration();
            hdfs = FileSystem.get(config);
            outputDir = config.get("outputPath");
            compressionCodec = config.get("compressionCodec");
            String buffersize = config.get("buffersize");
            buffer = Integer.parseInt(buffersize);


        }

        @Override
        public void cleanup(Context context) throws IOException {
            //hdfs.close();
        }

        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException{
            System.out.println("Mapper is about to process: " + value.toString());
            //System.out.println("The outputDir is " + outputDir);
            //System.out.println("The compression codec is " + compressionCodec);
            processSingleFile(new Path(value.toString()), context);
        }

        private void processSingleFile(Path sourceFilePath,
                                       Context context) throws IOException {

            context.getCounter("Files", "NumberOfFiles").increment(1);
            FileStatus fileStatus = hdfs.getFileStatus(sourceFilePath);
            context.getCounter("Files", "NumberOfFileBytes").increment(fileStatus.getLen());
            context.getCounter("Files", "NumberOfBlocks").increment(fileStatus.getLen() / fileStatus.getBlockSize());


            if (compressionCodec.equals("gzip")) {
                Path targetFilePath = new Path (String.format("%s/%s", outputDir, sourceFilePath.getName() + ".gz" ));
                writeGZip(sourceFilePath, targetFilePath, buffer);
            } else if (compressionCodec.equals("bzip2")){
                Path targetFilePath = new Path (String.format("%s/%s", outputDir, sourceFilePath.getName() + ".bz2" ));
                writeBZip2(sourceFilePath,targetFilePath);
            }


        }
        private void writeGZip (Path sourceFilePath, Path targetFilePath, int buffer) throws IOException {

            FSDataInputStream in = new FSDataInputStream(hdfs.open(sourceFilePath));
            FSDataOutputStream fileOut = hdfs.create(targetFilePath);
            GZIPOutputStream out = new GZIPOutputStream(fileOut, buffer);

            try {
                byte[] buf = new byte[1024];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
            } finally {
                in.close();
                out.finish();
                out.close();
            }

        }
        private void writeBZip2 (Path sourceFilePath, Path targetFilePath) throws IOException {

            FSDataInputStream in = new FSDataInputStream(hdfs.open(sourceFilePath));
            FSDataOutputStream fileOut = hdfs.create(targetFilePath);
            BZip2CompressorOutputStream out = new BZip2CompressorOutputStream(fileOut);

            try {
                byte[] buf = new byte[1024];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
            } finally {
                in.close();
                out.finish();
                out.close();
            }

        }
    }

}
