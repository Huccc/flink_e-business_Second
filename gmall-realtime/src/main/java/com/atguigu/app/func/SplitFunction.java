package com.atguigu.app.func;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    // 方法名不能变，是通过反射调用的
    public void eval(String str) {
        try {
            List<String> list = KeywordUtil.splitKeyWord(str);

            for (String word : list) {
                collect(Row.of(word));

            }

        } catch (IOException e) {
            System.out.println("发现异常数据");
            collect(Row.of(str));
        }
    }

}
