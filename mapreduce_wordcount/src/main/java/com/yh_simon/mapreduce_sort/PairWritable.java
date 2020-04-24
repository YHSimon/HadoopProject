package com.yh_simon.mapreduce_sort;

import lombok.Data;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class PairWritable implements WritableComparable<PairWritable> {


    private String first;
    private int second;

    @Override
    public String toString() {
        return
                "" + first + '\t' + "" + second;
    }

    //实现排序规则
    @Override
    public int compareTo(PairWritable other) { //默认升序
        //先first，若相同,再second比较
        int res = this.first.compareTo(other.first);
        if (res == 0) {
            return this.second - other.second;
        }
        return res;
    }

    //实现序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);
    }

    //实现反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }


}
