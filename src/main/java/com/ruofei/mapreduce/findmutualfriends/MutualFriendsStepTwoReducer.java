package com.ruofei.mapreduce.findmutualfriends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class MutualFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text> {

    Text newValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String temp = new String();

        // 循环遍历并拼接
        for (Text value : values) {

            if (temp.length() == 0) {
                temp = value.toString();
            } else {
                temp = temp + "," + value.toString();
            }
        }

        // 赋值
        newValue.set(temp);

        // 写出
        context.write(key, newValue);
    }
}
