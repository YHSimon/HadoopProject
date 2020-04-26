package com.yh_simon.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class MyUDF  extends UDF {
    public Text evaluate(final Text str){
        if(StringUtils.isNotBlank(str.toString())){
            String res = str.toString().substring(0, 1).toUpperCase() + str.toString().substring(1);
            return new Text(res);
        }
        return new Text("");
    }
}
