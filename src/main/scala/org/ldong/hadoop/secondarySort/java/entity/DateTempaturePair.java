package org.ldong.hadoop.secondarySort.java.entity;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author cssdongl@gmail.com
 * @version V1.0
 * @date 2017/3/22 14:09
 */
public class DateTempaturePair implements Writable, WritableComparable<DateTempaturePair> {
    private Text yearMonth = new Text();

    private Text day = new Text();

    private IntWritable tempature = new IntWritable();

    public DateTempaturePair() {
    }


    public DateTempaturePair(String yearMonth, String day, int tempature) {
        this.yearMonth.set(yearMonth);
        this.day.set(day);
        this.tempature.set(tempature);
    }

    @Override
    public int compareTo(DateTempaturePair o) {
        int compareValue = this.yearMonth.compareTo(o.getYearMonth());
        if (compareValue == 0) {
            compareValue = this.tempature.compareTo(o.getTempature());
        }
        //sort desc
        return -1 * compareValue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        yearMonth.write(out);
        day.write(out);
        tempature.write(out);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        day.readFields(in);
        tempature.readFields(in);
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth.set(yearMonth);
    }

    public Text getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day.set(day);
    }

    public IntWritable getTempature() {
        return tempature;
    }

    public void setTempature(int tempature) {
        this.tempature.set(tempature);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }

        DateTempaturePair that = (DateTempaturePair) obj;

        if (this.tempature != null ? (!this.tempature.equals(that.tempature)) : that.tempature != null) {
            return false;
        }

        if (this.yearMonth != null ? (!this.yearMonth.equals(that.yearMonth)) : that.yearMonth != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = yearMonth != null ? yearMonth.hashCode() : 0;

        result = 31 * result + (tempature != null ? tempature.hashCode() : 0);

        return result;
    }
}
