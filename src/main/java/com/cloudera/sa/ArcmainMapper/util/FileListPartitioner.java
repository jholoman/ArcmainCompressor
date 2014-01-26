package com.cloudera.sa.ArcmainMapper.util;

/**
 * Created by jholoman on 1/25/14.
 */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileListPartitioner extends Partitioner<Text, Text> implements Configurable
{
    public void setConf(Configuration conf) {
    }
    public Configuration getConf() {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks)
    {
        /* Pattern p = Pattern.compile("(.*)?part-m");
        Matcher m = p.matcher(key.toString());
        while (m.find()) {
           // System.err.println(m.group(1));
            return Math.abs(m.group(1).hashCode() % numReduceTasks);
        }*/
        return Math.abs(key.toString().hashCode() % numReduceTasks);
      //return 0;
    }
}




