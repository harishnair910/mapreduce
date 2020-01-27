package com.drkiettran.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.util.StringTokenizer;

/**
 * Extracted from Hadoop for Dummies (2014)
 */
public class FlightsByOrigins {
    private static Long totalFlights(String outputPath) throws IOException {
        String partFile = String.format("hdfs:%s/part-r-00000", outputPath);
        Path pt = new Path(partFile);// Location of file in HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        Long totalFlights = 0L;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)))) {
            String line = br.readLine();
            while (line != null) {
                StringTokenizer st = new StringTokenizer(line, " \t");
                String lastToken = null;
                while (st.hasMoreElements()) {
                    lastToken = st.nextToken();
                }
                totalFlights += Long.valueOf(lastToken);
                line = br.readLine();
            }
            return totalFlights;
        } finally {
            fs.close();
        }
    }

    private static void deleteOutputFolder(String folder) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        Path path = new Path(folder);
        fs.delete(path, true);
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            System.out.println("at least input file/directory and output directory");
            System.exit(-1);
        }

        String inputPath = argv[0];
        String outputPath = argv[1];
        deleteOutputFolder(outputPath);

        Job job = Job.getInstance();
        job.setJarByClass(FlightsByOrigins.class);
        job.setJobName("FlightsByOrigins");

        TextInputFormat.addInputPath(job, new Path(inputPath));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(FlightsByOriginsMapper.class);
        job.setReducerClass(FlightsByOriginsReducer.class);

        TextOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
        System.out.println("total flights:" + totalFlights(outputPath));
    }
}
