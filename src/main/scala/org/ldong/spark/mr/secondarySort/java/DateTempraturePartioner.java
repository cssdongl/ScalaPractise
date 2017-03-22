package org.ldong.spark.mr.secondarySort.java;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.ldong.spark.mr.secondarySort.java.entity.DateTempaturePair;
import java.lang.Math;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/3/22 14:44
 */
public class DateTempraturePartioner extends Partitioner<DateTempaturePair,Text> {
    @Override
    public int getPartition(DateTempaturePair dateTempaturePair, Text text, int numPartitions) {
        int partition = Math.abs(dateTempaturePair.getYearMonth().hashCode() % numPartitions);
        return partition;
    }
}
