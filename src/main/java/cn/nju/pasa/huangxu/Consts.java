package cn.nju.pasa.huangxu;

/**
 * @Author： huangxu.chase
 * Email: huangxu@smail.nju.edu.cn
 * @Date： 2022/4/20
 * @description：
 */
public interface Consts {
    int TEST_NUM_PARTITIONS = 5;

    int TEST_BATCH_KEY_RANGE = 10_000_000;
    int TEST_BATCH_COLUMN_NUM = 3;
    int TEST_BATCH_COLUMN_VALUE_RANGE = 10_000_000;
    int TEST_BATCH_RECORDS_NUM = 201326592;  // 201326592 single partition, 3GB / 16B reocrd

    int TEST_STREAM_KEY_RANGE = 10_000_000;
    int TEST_STREAM_COLUMN_VALUE_RANGE = 100_000;
    int TEST_STREAM_RECORDS_NUM = 223696213; // 223696213 single partition, 5GB / 24B record

    String MASTER_IP = "192.168.100.213";
    String HADOOP_PORT = "9000";

    String BTREE_DIR_LINUX = "/home/huangxu/bench/btree/";
    String BTREE_DIR_MAC = "/Users/chase/Desktop/pasa_projects/flink_join_ud/output/bplustree/";

    String BatchDataPath = "/data_generate/output_3GB/";
    String BatchDataPathTest = "/data_generate/output/";
    String BatchDataPathInLocal = "/Users/chase/Desktop/pasa_projects/flink_join_ud/output/data_generate_output/";

}
