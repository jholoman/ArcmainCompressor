package com.cloudera.sa.ArcmainMapper.util;

/**
 * Created by jholoman on 1/22/14.
 */
import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FixedLengthRecordReader  extends RecordReader<LongWritable, BytesWritable> {

    private final int recordByteLength;
    private long start;
    private long pos;
    private long end;
    private byte[] currentRecord;
   private DataOutputBuffer buffer = new DataOutputBuffer();



    private LongWritable key = null;
    private BytesWritable value = null;

    BufferedInputStream fileIn;

    FixedLengthRecordReader ( int recordByteLength) throws IOException {
        this.recordByteLength = recordByteLength;

    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        currentRecord = new byte[recordByteLength];
        FileSplit split = (FileSplit) genericSplit;

        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        FileSystem fs = file.getFileSystem(context.getConfiguration());
        fileIn = new BufferedInputStream(fs.open(split.getPath()));


       if (start != 0) {
            pos = start - (start % recordByteLength) + recordByteLength;

            fileIn.skip(pos);
        }


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (pos >= end) {
            return false;
        }

        if (key == null) {
            key = new LongWritable();
        }
        if (value == null) {
            value = new BytesWritable();
        }

        int result = fileIn.read(currentRecord);

        pos += result;

        if (result == 0) {
            return false;
        } else if (result < recordByteLength) {
             key.set(result);
            value.set(currentRecord, 0, result);
            System.out.println("Returning Short Record:" + result + " " + currentRecord.length);
            return true;
        } else {
            key.set(result);
            value.set(currentRecord, 0, result);
            return true;
        }



    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float)(end - start));
        }
    }

    @Override
    public void close() throws IOException {
        fileIn.close();
    }



}
