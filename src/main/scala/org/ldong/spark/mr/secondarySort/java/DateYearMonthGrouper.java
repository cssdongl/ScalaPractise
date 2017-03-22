package org.ldong.spark.mr.secondarySort.java;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.ldong.spark.mr.secondarySort.java.entity.DateTempaturePair;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/3/22 14:54
 */
public class DateYearMonthGrouper extends WritableComparator {
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        DateTempaturePair dt1 = (DateTempaturePair)a;
        DateTempaturePair dt2 = (DateTempaturePair)b;
        return dt1.getYearMonth().compareTo(dt2.getYearMonth());
    }
}
