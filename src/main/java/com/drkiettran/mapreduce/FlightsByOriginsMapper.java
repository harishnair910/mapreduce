package com.drkiettran.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Extracted from Hadoop for Dummies (2014)
 */
public class FlightsByOriginsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightsByOriginsMapper.class);
    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /**
         * Note that the key is the index of the file where the companion value (or line)
         * is located. We are skipping the first line of text where the column headings
         * are located. Thus, skip processing when the index is 0.
         */
        if (key.get() > 0) {
            String[] lines = new CSVParser().parseLine(value.toString());
            /* the 8th index is that for the name of airline carrier */
            Text originatedCity = new Text(lines[16]);
            context.write(originatedCity, ONE);
        } else {
            LOGGER.info("skip csv columns heading!");
        }
    }
}