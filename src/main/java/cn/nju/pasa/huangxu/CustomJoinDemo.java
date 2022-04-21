package cn.nju.pasa.huangxu;

import btree4j.*;
import btree4j.indexer.BasicIndexQuery;
import btree4j.utils.lang.Primitives;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


import java.io.File;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author： huangxu.chase
 * Email: huangxu@smail.nju.edu.cn
 * @Date： 2022/4/19
 * @description：
 */
public class CustomJoinDemo {
    static class CustomSource implements SourceFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> {
        private boolean running = true;

        @Override
        public void run(SourceContext sourceContext) throws Exception {
            Random random = new Random();
            long cnt = 0;
            ZipfGenerator zipf = new ZipfGenerator(Consts.TEST_STREAM_KEY_RANGE, 0.3);;
            while (this.running && cnt < Consts.TEST_NUM_PARTITIONS * Consts.TEST_STREAM_RECORDS_NUM) {
                Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> values = new Tuple6<>();
                int key = zipf.next();
                values.f0 = key;
                for (int i = 0; i < 5; i++) {
                    values.setField(random.nextInt(Consts.TEST_STREAM_COLUMN_VALUE_RANGE), i + 1);
                }
                sourceContext.collect(values);
                cnt++;
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }

    public static class CustomPartitioner implements Partitioner<Integer> {
        int singlePartitionRange = -1;

        @Override
        public int partition(Integer key, int numPartitions) {
            if (singlePartitionRange == -1) {
                singlePartitionRange = Consts.TEST_STREAM_KEY_RANGE / numPartitions;
            }
            return key / singlePartitionRange;
        }
    }

    public static class CustomKeySelector implements KeySelector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Integer> {

        @Override
        public Integer getKey(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> values) throws Exception {
            return values.f0;
        }
    }

    /**
     * Two storage format，KV store(unique key)，relation table store(duplicate key)
     * @param <K>
     * @param <V>
     */
    interface CustomStorage<K, V> {
        void appendAsKV(K k, V v);
        void appendAsList(K k, List<V> v);
        V getAsKV(K k);
        List<List<V>> getAsList(K k);
        boolean contains(K k);
    }

    public static class BPlusTreeStorage implements CustomStorage<Integer, Integer> {
        private BTreeIndex btree = null;

        public BPlusTreeStorage() {
            // 别用这个构造函数
        }

        public BPlusTreeStorage(int partitionIdx) throws BTreeException {
            String uuid = UUID.randomUUID().toString().replaceAll("-", "");
            String osName = System.getProperty("os.name");
            String btreeFilePath = "";
            if (osName.contains("Mac")) {
                btreeFilePath = Consts.BTREE_DIR_MAC;
            } else {
                btreeFilePath = Consts.BTREE_DIR_LINUX;
            }
//            btreeFilePath += "BPlusTree_" + uuid + ".idx";
            btreeFilePath += "BPlusTree_" + partitionIdx + ".idx";
            File tmpFile = new File(btreeFilePath);
//            tmpFile.deleteOnExit();

            btree = new BTreeIndex(tmpFile);
            btree.init(/* bulkload */ false);
        }

        public BPlusTreeStorage(String filePath) throws BTreeException {
            File tmpFile = new File(filePath);
            btree = new BTreeIndex(tmpFile);
            btree.init(/* bulkload */ false);
        }

        @Override
        public void appendAsKV(Integer k, Integer v) {
            try {
                if (contains(k)) {
                    btree.remove(new Value(k));
                }
                btree.addValue(new Value(k), new Value(v));
            } catch (BTreeException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void appendAsList(Integer k, List<Integer> v) {
            List<String> values = new ArrayList<>();
            String value = v.stream().map(String::valueOf).collect(Collectors.joining(","));
            try {
                btree.addValue(new Value(k), new Value(value));
            } catch (BTreeException e) {
                e.printStackTrace();
            }
        }

        @Override
        public Integer getAsKV(Integer k) {
            try {
                return (int) Primitives.getLong(btree.getValueBytes(k));
            } catch (BTreeException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public List<List<Integer>> getAsList(Integer k) {
            List<List<Integer>> res = new ArrayList<>();
            try {
                btree.search(new BasicIndexQuery.IndexConditionEQ(new Value(k)), new BTreeCallback() {
                    public boolean indexInfo(Value value, long pointer) {
                        throw new UnsupportedOperationException();
                    }

                    public boolean indexInfo(Value key, byte[] value) {
                        char[] dest = new char[value.length / 2];
                        Primitives.getChars(value, dest);
                        String vv = new String(dest);
                        List<Integer> row = Arrays.stream(vv.split(",")).map(Integer::parseInt)
                                .collect(Collectors.toList());
                        res.add(row);
                        return true;
                    }
                });
                return res;
            } catch (BTreeException e) {
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public boolean contains(Integer k) {
            try {
                return btree.findValue(new Value(k)) != BTree.KEY_NOT_FOUND;
            } catch (BTreeException e) {
                e.printStackTrace();
            }
            return false;
        }
    }


    public static class CustomJoinFunction extends RichFlatMapFunction<List<Integer>, List<Integer>> {
        CustomStorage<Integer, Integer> storage;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            try {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                storage = new BPlusTreeStorage(indexOfThisSubtask);
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                System.out.println("begin open file at " + indexOfThisSubtask);
                FileSystem fs = FileSystem.get(new URI("hdfs://" + Consts.MASTER_IP + ":" + Consts.HADOOP_PORT), conf, "root");
                FSDataInputStream fsDataInputStream = fs.open(new Path(Consts.BatchDataPath + "part-r-0000" + indexOfThisSubtask));
                List<Integer> rowData = new ArrayList<>();
                for (int i = 0; i < Consts.TEST_BATCH_COLUMN_NUM; i++) {
                    rowData.add(0);
                }
                while (fsDataInputStream.available() > 0) {
                    String line = fsDataInputStream.readLine();

                    String[] splits = line.split("\t");
                    if (splits.length != 2) {
                        continue;
                    }
                    int key = Integer.parseInt(splits[0]);
                    String[] values = splits[1].split(",");
                    for (int i = 0; i < values.length; i++) {
                        rowData.set(i, Integer.parseInt(values[i]));
                    }
                    storage.appendAsList(key, rowData);
                }
                System.out.println("end open file at " + indexOfThisSubtask);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        public void flatMap(List<Integer> value, Collector<List<Integer>> out) throws Exception {
            if (!storage.contains(value.get(0))) {
                return;
            }
            storage.getAsList(value.get(0)).forEach(batchTableValue -> {
                ArrayList<Integer> joinRow = new ArrayList<>();
                joinRow.addAll(value);
                joinRow.addAll(batchTableValue);
                out.collect(joinRow);
            });
        }
    }


    public static void main(String[] args) throws Exception {
        // 5元素的流表 join 4元素的批表
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Consts.TEST_NUM_PARTITIONS);
        env.disableOperatorChaining();

        env.addSource(new CustomSource()).partitionCustom(new CustomPartitioner(), new CustomKeySelector())
                .map(streamValue -> {
                    List<Integer> values = new ArrayList<>();
                    for(int i = 0; i < 6; i++){
                        values.add(streamValue.getField(i));
                    }
                    return values;
                }).returns(Types.LIST(Types.INT))
                .flatMap(new CustomJoinFunction()).addSink(new DiscardingSink<>());

        env.execute("MyJoin");
    }
}