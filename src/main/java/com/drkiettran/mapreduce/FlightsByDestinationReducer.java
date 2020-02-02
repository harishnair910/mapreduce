package com.drkiettran.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FlightsByDestinationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlightsByCarriersReducer.class);

    @Override
    protected void reduce(Text token, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable count : counts) {
            sum += count.get();
        }
        context.write(token, new IntWritable(sum));
        LOGGER.info("{}: {}", token, sum);
    }
}
