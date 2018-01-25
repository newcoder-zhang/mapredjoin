package com.mapjoin.www;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MyMapJoin extends Configured implements Tool{
    //注意和配置文件中的端口号core-site.xml中一致
    public static String customer_cache_url="hdfs://beifeng:8020/user/beifeng/mapreduce/cache/customers.csv";



    public static class MapJoinMapper extends Mapper<LongWritable,Text,CustomerOrder,Text>{
        private Map<Integer,Customer> customers=new HashMap<>();
        private CustomerOrder cusord=new CustomerOrder();
        private Text mapoutvalue=new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.newInstance(URI.create(customer_cache_url),context.getConfiguration());
            FSDataInputStream fsis = fs.open(new Path(customer_cache_url));
            BufferedReader reader=new BufferedReader(new InputStreamReader(fsis));
            String line=null;
            while((line=reader.readLine())!=null){
                String[] strs = line.split(",");
                if(strs.length<3){
                    continue;
                }
                int cid= Integer.valueOf(strs[0]);
                customers.put(cid,new Customer(cid,strs[1],strs[2]));
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values=value.toString().split(",");
            if(values.length<4){
                return;
            }
            int cid=Integer.valueOf(values[1]);//map处理的是order的数据,第二列是cid
            Customer customer = customers.get(cid);
            if(customer==null){
                return;
            }
            StringBuilder sb=new StringBuilder();
            sb.append(values[2])
                    .append(",")
                    .append(values[3])
                    .append(",")
                    .append(customer.getName())
                    .append(",")
                    .append(customer.getTel());
            cusord.set(cid,Integer.valueOf(values[0]));
            mapoutvalue.set(sb.toString());
            context.write(cusord,mapoutvalue);
        }
    }

    public static class MapJoinReduce extends Reducer<CustomerOrder,Text,CustomerOrder,Text>{
        @Override
        protected void reduce(CustomerOrder key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text:values) {
                context.write(key,text);
            }
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job=Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.addCacheFile(URI.create(customer_cache_url));
        job.setMapperClass(MapJoinMapper.class);
        job.setReducerClass(MapJoinReduce.class);

        FileInputFormat.addInputPath(job,new Path(strings[0]));
        FileOutputFormat.setOutputPath(job,new Path(strings[1]));

        job.setMapOutputKeyClass(CustomerOrder.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(CustomerOrder.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true)?0:1;
    }
    public static void main(String[] args) throws Exception {
        int r= ToolRunner.run(new Configuration(),new MyMapJoin(),args);
        System.exit(r);
    }
}
