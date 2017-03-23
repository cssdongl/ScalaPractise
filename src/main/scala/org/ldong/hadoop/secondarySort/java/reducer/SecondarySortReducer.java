package org.ldong.hadoop.secondarySort.java.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.ldong.hadoop.secondarySort.java.entity.DateTempaturePair;

import java.io.IOException;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/3/22 14:49
 */
public class SecondarySortReducer extends Reducer<DateTempaturePair,Text,Text,Text> {

    @Override
    protected void reduce(DateTempaturePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for(Text value : values){
            //the temparture values
            sb.append(value.toString());
            sb.append(",");
        }
        context.write(key.getYearMonth(),new Text(sb.toString()));
    }
}
