package org.ldong.hadoop.secondarySort.java.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.ldong.hadoop.secondarySort.java.DateTempraturePartioner;
import org.ldong.hadoop.secondarySort.java.DateYearMonthGrouper;
import org.ldong.hadoop.secondarySort.java.entity.DateTempaturePair;
import org.ldong.hadoop.secondarySort.java.mapper.SecondarySortMapper;
import org.ldong.hadoop.secondarySort.java.reducer.SecondarySortReducer;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/3/22 10:23
 */
public class SecondarySortJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Job job = new Job(conf, "The Secondary sort mapreduce job");

        job.setJarByClass(SecondarySortJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(DateTempaturePair.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTempraturePartioner.class);
        job.setGroupingComparatorClass(DateYearMonthGrouper.class);

        int result = job.waitForCompletion(true) ? 0 : 1;

        return result;

    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SecondarySortJob(), args);
    }
}
