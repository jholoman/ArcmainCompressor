package com.cloudera.sa.ArcmainMapper;

import java.io.*;

import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.commons.compress.compressors.bzip2.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.cloudera.sa.ArcmainMapper.util.ConfigurableInputFormat;


public class FileUnCompressorJob {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out
                    .println("FileCompressorJob <inputPath> <outputPath> <# mappers> ");
            System.out.println();
            System.out
                    .println("Example: FileUnCompressorJob ./input ./output 20 gizp");
            return;
        }

        // Get values from args
        String inputPath = args[0];
        String outputPath = args[1];
        String numberOfMappers = args[2];
        //String compressionCodec = args[3];


        Configuration conf = new Configuration();
        conf.set("outputPath", outputPath);

        // Create job
        Job job = new Job(conf);
        job.setJobName("FileUnCompressorJob");


        job.setJarByClass(FileUnCompressorJob.class);
        // Define input format and path
        job.setInputFormatClass(ConfigurableInputFormat.class);
        ConfigurableInputFormat.setInputPath(job, inputPath);
        ConfigurableInputFormat.setMapperNumber(job, Integer.parseInt(numberOfMappers));

        // Define output format
        job.setOutputFormatClass(NullOutputFormat.class);
        // Define the mapper and reducer
        job.setMapperClass(UncompressMapper.class);
        // job.setReducerClass(Reducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);
/// Do we really want this here???
        //   hdfs.delete(new Path(outputPath), true);

        // Exit
        job.waitForCompletion(true);
    }

    public static class UncompressMapper extends
            Mapper<LongWritable, Text, BytesWritable, BytesWritable> {
        Text newKey = new Text();
        Text newValue = new Text();
        String outputDir = new String();
        String compressionCodec = new String();
        Configuration config;
        FileSystem hdfs;


        @Override
        public void setup(Context context) throws IOException {
            //config = new Configuration();
            config = context.getConfiguration();
            hdfs = FileSystem.get(config);
            outputDir = config.get("outputPath");
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
                                       Context context) throws IOException, InterruptedException {

            context.getCounter("Files", "NumberOfFiles").increment(1);
            FileStatus fileStatus = hdfs.getFileStatus(sourceFilePath);
            context.getCounter("Files", "NumberOfFileBytes").increment(fileStatus.getLen());
            context.getCounter("Files", "NumberOfBlocks").increment(fileStatus.getLen() / fileStatus.getBlockSize());

            String ext = "";
            String originalFileName = sourceFilePath.getName();
            String newFileName = "";
            // get the extension:

            int i  = sourceFilePath.getName().lastIndexOf('.');
            if (i > 0) {
                ext = originalFileName.substring(i+1);
                newFileName = originalFileName.substring(0, i);
            }

            Path targetFilePath = new Path (String.format("%s/%s", outputDir, newFileName ));


            if (ext.equals("gz")) {
                uncompressGZip(sourceFilePath, targetFilePath);
            } else if (ext.equals("bz2")) {
                uncompressBZip2(sourceFilePath, targetFilePath);
            }
              


        }
        private void uncompressGZip (Path sourceFilePath, Path targetFilePath) throws IOException {

            FSDataInputStream fileIn = new FSDataInputStream(hdfs.open(sourceFilePath));
            GZIPInputStream in = new GZIPInputStream(fileIn);
            FSDataOutputStream out = hdfs.create(targetFilePath);

            //GZIPOutputStream out = new GZIPOutputStream(fileOut);

            try {
                byte[] buf = new byte[1024];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
            } finally {
                in.close();
                out.close();
            }

        }
        private void uncompressBZip2 (Path sourceFilePath, Path targetFilePath) throws IOException {

            FSDataInputStream fileIn = new FSDataInputStream(hdfs.open(sourceFilePath));
            BZip2CompressorInputStream in = new BZip2CompressorInputStream(fileIn);
            FSDataOutputStream out = hdfs.create(targetFilePath);

            try {
                byte[] buf = new byte[1024];
                int len;
                while ((len = in.read(buf)) > 0) {
                    out.write(buf, 0, len);
                }
            } finally {
                in.close();
                out.close();
            }

        }
    }

}
