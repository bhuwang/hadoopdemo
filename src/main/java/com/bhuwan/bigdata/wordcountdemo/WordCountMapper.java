/**
 * 
 */
package com.bhuwan.bigdata.wordcountdemo;

import java.io.IOException;
import java.util.StringTokenizer;

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
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Text word = new Text();
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line, "|");
        while (tokenizer.hasMoreTokens()) {
          word.set(tokenizer.nextToken());
          context.write(word, new IntWritable(1));
        }
    }
}
