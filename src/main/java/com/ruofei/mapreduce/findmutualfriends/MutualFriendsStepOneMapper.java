package com.ruofei.mapreduce.findmutualfriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MutualFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text> {

    Text newKey = new Text();
    Text newValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 取出一行
        String line = value.toString();

        // 分割
        String[] fileds = line.split(":");

        // 取出账户名以及好友名
        String person = fileds[0];
        String[] friends = fileds[1].split(",");

        // 输出：key 好友名， value 账户名
        for (String friend : friends) {

            newKey.set(friend);
            newValue.set(person);
            context.write(newKey, newValue);
        }
    }
}
