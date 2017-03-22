package org.ldong.spark.mr.secondarySort.java.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.ldong.spark.mr.secondarySort.java.entity.DateTempaturePair;

import java.io.IOException;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/3/22 14:18
 */
public class SecondarySortMapper extends Mapper<LongWritable,Text,DateTempaturePair,Text> {

    private DateTempaturePair pair = new DateTempaturePair();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //year,month,day,tempature
        String[] keyParts = value.toString().split(",");

        String yearMonth = keyParts[0] + keyParts[1];

        String day = keyParts[2];

        int temperature = Integer.parseInt(keyParts[3]);

        pair.setYearMonth(yearMonth);
        pair.setDay(day);
        pair.setTempature(temperature);

        context.write(pair,new Text(keyParts[3]));

    }
}
