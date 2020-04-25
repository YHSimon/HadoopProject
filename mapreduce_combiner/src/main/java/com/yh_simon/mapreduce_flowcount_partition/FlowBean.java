package com.yh_simon.mapreduce_flowcount_partition;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlowBean implements Writable {//仅实现序列化
    private Long upFlow;
    private Long downFlow;
    private Long upCountFlow;
    private Long downCountFlow;

    @Override
    public String toString() {
        return
               upFlow +
                "\t" + downFlow +
                "\t" + upCountFlow +
                "\t" + downCountFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(upCountFlow);
        dataOutput.writeLong(downCountFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow=dataInput.readLong();
        this.downFlow=dataInput.readLong();
        this.upCountFlow=dataInput.readLong();
        this.downCountFlow=dataInput.readLong();
    }
}
