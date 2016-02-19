/**
 * 
 */
package com.bhuwan.bigdata.wordcountdemo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author bhuwan
 *
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {

        String[] values = value.toString().split("|");
        for (String val : values) {
            context.write(new Text(val), new IntWritable(1));
        }
    }
}
