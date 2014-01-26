package com.cloudera.sa.ArcmainMapper;

import java.io.*;

import java.util.zip.GZIPInputStream;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.cloudera.sa.ArcmainMapper.util.ConfigurableInputFormat;
import com.cloudera.sa.ArcmainMapper.util.FileListPartitioner;


public class FileSplitUnCompressorJob {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out
                    .println("FileSplitUnCompressorJob <inputPath> <outputPath>");
            System.out.println();
            System.out
                    .println("Example: FileSplitUnCompressorJob ./input ./output ");
            return;
        }

        // Get values from args
        String inputPath = args[0];
        String outputPath = args[1];
        String numberOfMappers = "1";

        Configuration conf = new Configuration();
        conf.set("outputPath", outputPath);

        // Create job
        Job job = new Job(conf);
        job.setJobName("FileSplitUnCompressorJob");

        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(inputPath));

        Set<String> files = new HashSet<String>();

        Pattern p = Pattern.compile("(.*)?-part-m");

        for (FileStatus fstatus : status) {
            Matcher m = p.matcher(fstatus.getPath().getName());
            while (m.find()) {
                files.add(m.group(1));
            }
        }
        job.setNumReduceTasks(files.size());


        job.setJarByClass(FileSplitUnCompressorJob.class);
        // Define input format and path
        job.setInputFormatClass(ConfigurableInputFormat.class);
        ConfigurableInputFormat.setInputPath(job, inputPath);
        ConfigurableInputFormat.setMapperNumber(job, Integer.parseInt(numberOfMappers));


        // Define output format
        job.setOutputFormatClass(NullOutputFormat.class);
        // Define the mapper and reducer
        job.setMapperClass(UncompressMapper.class);
        job.setReducerClass(UncompressReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setPartitionerClass(FileListPartitioner.class);

        job.waitForCompletion(true);
    }

    public static class UncompressMapper extends
            Mapper<LongWritable, Text, Text, Text> {
        String outputDir;
        Configuration config;
        FileSystem hdfs;


        @Override
        public void setup(Context context) throws IOException {
            config = context.getConfiguration();
            hdfs = FileSystem.get(config);
            outputDir = config.get("outputPath");
        }

        @Override
        public void cleanup(Context context) throws IOException {
        }

        @Override
        public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException{
            System.out.println("Mapper is about to process: " + value.toString());

            Path sourceFilePath = new Path(value.toString());

            Pattern p = Pattern.compile("(.*)?-part-m");
            Matcher m = p.matcher(sourceFilePath.getName());
            String newVal = "";
            while (m.find()) {
                newVal = m.group(1);
                //System.out.println(m.group(1));
            }
            context.write(new Text(newVal), new Text(value.toString()));
        }

    }

    public static class UncompressReducer extends Reducer <Text, Text, Text, Text>   {
        String outputDir;
        Configuration config;
        FileSystem hdfs;

        @Override
        public void setup(Context context) throws IOException {
            config = context.getConfiguration();
            hdfs = FileSystem.get(config);
            outputDir = config.get("outputPath");
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //System.out.println("The key is " + key.toString());

            Path targetFile = new Path(outputDir + "/" + key.toString());
            FSDataOutputStream out = hdfs.create(targetFile);

            Iterator<Text> iter = values.iterator();
            while (iter.hasNext()) {
                Path path = new Path (iter.next().toString());
                System.out.println("The path to process is "  + path.toString());
                FSDataInputStream fileIn = new FSDataInputStream(hdfs.open(path));
                GZIPInputStream in = new GZIPInputStream(fileIn);

                try {
                    byte[] buf = new byte[1024];
                    int len;
                    while ((len = in.read(buf)) > 0) {
                        out.write(buf, 0, len);
                    }
                } finally {
                    in.close();
                }
                            }
            out.close();
            System.out.println("Finished---");

        }
    }
}
