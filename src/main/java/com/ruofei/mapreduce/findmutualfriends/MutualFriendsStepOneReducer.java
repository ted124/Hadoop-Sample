package com.ruofei.mapreduce.findmutualfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class MutualFriendsStepOneReducer extends Reducer<Text, Text, Text, Text> {

    // 新的key
    Text newKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 建立一个list储存账户名称
        ArrayList<String> pastValues = new ArrayList<String>();

        // 遍历所有的value
        for (Text value : values) {

            // 如果list不为空，就进行遍历
            if (pastValues.size() != 0) {

                // 遍历所有的pastValue
                for (String pastValue : pastValues) {

                    // 判断pastValue和Value的先后顺序
                    if (pastValue.compareTo(value.toString()) < 0) {

                        // 新的key值
                        newKey.set(pastValue + "-" + value.toString());
                    } else {

                        // 新的key值
                        newKey.set(value.toString() + "-" + pastValue);
                    }
                    // 写出
                    context.write(newKey, key);
                }
            }

            // 将value加入到pastValues中
            pastValues.add(value.toString());
        }
    }
}
