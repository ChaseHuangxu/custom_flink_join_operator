package cn.nju.pasa.huangxu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @Author： huangxu.chase
 * Email: huangxu@smail.nju.edu.cn
 * @Date： 2022/4/21
 * @description：
 */
public class DataGenerateMR {
    private static int MAPPER_NUM = 100;

    public static class DGMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            ZipfGenerator zipf = new ZipfGenerator(Consts.TEST_BATCH_KEY_RANGE, 0.3);
            Random random = new Random();
            LongWritable key = new LongWritable();
            Text value = new Text();

            List<Integer> columnsList = new ArrayList<>();
            for (int i = 0; i < Consts.TEST_BATCH_COLUMN_NUM; i++) {
                columnsList.add(0);
            }
            for (int i = 0; i < Consts.TEST_BATCH_RECORDS_NUM * Consts.TEST_NUM_PARTITIONS / MAPPER_NUM; i++) {
                int curKey = zipf.next();
                columnsList.set(0, curKey);
                for (int j = 0; j < Consts.TEST_BATCH_COLUMN_NUM; j++) {
                    columnsList.set(j, random.nextInt(Consts.TEST_BATCH_COLUMN_VALUE_RANGE));
                }
                key.set(curKey);
                value.set(columnsList.stream().map(String::valueOf).collect(Collectors.joining(",")));
                context.write(key, value);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {

        }
    }

    public static class DGReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            for (Text value: values) {
                context.write(key, value);
            }
        }
    }

    public static class DGPartitioner extends Partitioner<LongWritable, Text> {
        private ZipfGenerator zipf = new ZipfGenerator(Consts.TEST_BATCH_KEY_RANGE, 0.3);
        List<Integer> separatorNums = zipf.getSeparatorNum(Consts.TEST_NUM_PARTITIONS);

        @Override
        public int getPartition(LongWritable k, Text v, int numPartitions) {
            int partitionIdx = Consts.TEST_NUM_PARTITIONS - 1;
            for(int j = 0; j < separatorNums.size(); j ++) {
                if (k.get() <= separatorNums.get(j)) {
                    partitionIdx = j;
                    break;
                }
            }
            return partitionIdx;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.100.213:9000"), conf, "root");
        fs.delete(new Path(args[0]), true);
        fs.delete(new Path(args[1]), true);
        fs.mkdirs(new Path(args[0]));
        for (int i = 0; i < MAPPER_NUM; i++) {
            FSDataOutputStream outputStream = fs.create(new Path(args[0] + "/" + i + ".txt"));
            outputStream.writeChars("aaa");
            outputStream.close();
        }
        fs.close();

        conf = new Configuration();
        Job job = Job.getInstance(conf, DataGenerateMR.class.getSimpleName());
        job.setJarByClass(DataGenerateMR.class);

        FileInputFormat.setInputPaths(job, args[0]);
        job.setMapperClass(DGMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(DGPartitioner.class);

        job.setReducerClass(DGReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(Consts.TEST_NUM_PARTITIONS);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
