package com.ruofei.mapreduce.findmutualfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MutualFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text newKey = new Text();
    Text newValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 读取
        String line = value.toString();

        // 切割
        String[] fileds = line.split("\t");

        // 取出对应的值
        newKey.set(fileds[0]);
        newValue.set(fileds[1]);

        // 输出
        context.write(newKey, newValue);
    }
}
