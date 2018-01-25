package com.reducejoin.www;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Vector;

public class MyRedJoin extends Configured implements Tool {
    public static Text mapoutkey = new Text();
    public static Text mapoutvalue = new Text();
    public static Text redoutvalue = new Text();

    public static class RedJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取输入的split的文件名
            FileSplit split = (FileSplit) context.getInputSplit();
            String pname = split.getPath().getName();

            String data = value.toString();
            if (pname.contains("customers")) {
                String[] datas = data.split(",");
                if (datas.length < 3) return;
                String cid = datas[0];
                String name = datas[1];
                String tel = datas[2];
                mapoutkey.set(cid);
                mapoutvalue.set("c#" + name + "," + tel);
            } else if (pname.contains("orders")) {
                String[] datas = data.split(",");
                if (datas.length < 4) return;
                String oid = datas[0];
                String cid = datas[1];
                String price = datas[2];
                String date = datas[3];
                mapoutkey.set(cid);
                mapoutvalue.set("o#" + oid + "," + price + "," + date);
            }
            context.write(mapoutkey, mapoutvalue);
        }
    }

    public static class RedJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Vector<String> vc = new Vector<>();
            Vector<String> vo = new Vector<>();
            for (Text value : values) {
                String data = value.toString();
                if (data.startsWith("c#")) {
                    vc.add(data.substring(2));
                } else if (data.startsWith("o#")) {
                    vo.add(data.substring(2));
                }

            }

            for (int i = 0; i < vc.size(); i++) {//笛卡尔积
                for (int j = 0; j < vo.size(); j++) {
                    redoutvalue.set(vc.get(i) + "," + vo.get(j));
                    context.write(key, redoutvalue);
                }
            }

        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        conf.set("mapreduce.output.textoutputformat.separator",",");
        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setMapperClass(RedJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(RedJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        //args = new String[]{"/user/beifeng/mapreduce/cache/*", "output9"};
        System.exit(ToolRunner.run(new Configuration(), new MyRedJoin(), args));
    }
}
