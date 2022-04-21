package cn.nju.pasa.huangxu;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author： huangxu.chase
 * Email: huangxu@smail.nju.edu.cn
 * @Date： 2022/4/21
 * @description：
 */
public class OriginalJoinDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(Consts.TEST_NUM_PARTITIONS);

        SingleOutputStreamOperator<List<Integer>> batchDatas = env.readTextFile(Consts.BatchDataPath).map(line -> {
            List<Integer> rowDatas = new ArrayList<>();
            String[] splits = line.split("\t");
            if (splits.length != 2) {
                return rowDatas;
            }

            rowDatas.add(Integer.parseInt(splits[0]));
            for (String value : splits[1].split(",")) {
                rowDatas.add(Integer.parseInt(value));
            }
            return rowDatas;
        }).returns(Types.LIST(Types.INT)).filter(row -> row!= null && row.size() > 0);
        SingleOutputStreamOperator<List<Integer>> streamDatas = env.addSource(new CustomJoinDemo.CustomSource()).partitionCustom(new CustomJoinDemo.CustomPartitioner(), new CustomJoinDemo.CustomKeySelector())
                .map(streamValue -> {
                    List<Integer> values = new ArrayList<>();
                    for (int i = 0; i < 6; i++) {
                        values.add(streamValue.getField(i));
                    }
                    return values;
                }).returns(Types.LIST(Types.INT));

        DataStream<List<Integer>> joinDataStream = streamDatas.join(batchDatas)
                .where(record -> record.get(0))
                .equalTo(record -> record.get(0))
                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
                .apply(new JoinFunction<List<Integer>, List<Integer>, List<Integer>>() {
                    @Override
                    public List<Integer> join(List<Integer> integers, List<Integer> integers2) throws Exception {
                        ArrayList<Integer> joinRow = new ArrayList<>();
                        joinRow.addAll(integers);
                        joinRow.addAll(integers2.subList(1, integers2.size()));
                        return joinRow;
                    }
                });

        joinDataStream.addSink(new PrintSinkFunction<>());

        env.execute("Original Join");
    }
}
