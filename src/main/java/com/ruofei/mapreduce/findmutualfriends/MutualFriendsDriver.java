package com.ruofei.mapreduce.findmutualfriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MutualFriendsDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job1 = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job1.setJarByClass(MutualFriendsDriver.class);

        // 3 设置map和reduce类
        job1.setMapperClass(MutualFriendsStepOneMapper.class);
        job1.setReducerClass(MutualFriendsStepOneReducer.class);

        // 4 设置map输出
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // 5 设置最终输出kv类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        // 7 提交
        boolean result1 = job1.waitForCompletion(true);

        if (!result1) {
            System.exit(1);
        }

        // 1 获取配置信息以及封装任务
        Job job2 = Job.getInstance(configuration);

        // 2 设置jar加载路径
        job2.setJarByClass(MutualFriendsDriver.class);

        // 3 设置map和reduce类
        job2.setMapperClass(MutualFriendsStepTwoMapper.class);
        job2.setReducerClass(MutualFriendsStepTwoReducer.class);

        // 4 设置map输出
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        // 5 设置最终输出kv类型
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // 7 提交
        boolean result2 = job2.waitForCompletion(true);

        System.exit(result2 ? 0 : 1);

    }

}
