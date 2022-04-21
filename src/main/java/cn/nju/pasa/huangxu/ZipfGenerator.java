package cn.nju.pasa.huangxu;

import scala.Int;
import xsbti.api._internalOnly_NameHash;

import java.io.Serializable;
import java.util.*;

/**
 * @Author： huangxu.chase
 * Email: huangxu@smail.nju.edu.cn
 * @Date： 2022/4/20
 * @description：
 */

public class ZipfGenerator implements Serializable {
    private Random random = new Random(0);
    private NavigableMap<Double, Integer> map;
    private static final double Constant = 1.0;
    private static final int sampleSize = 5000;
    private static int BOUND = 0;

    public ZipfGenerator(int R, double F) {
        BOUND = R;
        map = computeMap(R, F);
    }

    public static List<Integer> getRandomNumList(int nums,int bound){
        List<Integer> list = new ArrayList<>();

        Random r = new Random();
        while(list.size() != nums){
            int num = r.nextInt(bound);
            if(!list.contains(num)){
                list.add(num);
            }
        }
        Collections.sort(list);
        return list;
    }

    //size为rank个数，skew为数据倾斜程度, 取值为0表示数据无倾斜，取值越大倾斜程度越高
    private static NavigableMap<Double, Integer> computeMap(
            int size, double skew) {
        NavigableMap<Double, Integer> map =
                new TreeMap<Double, Integer>();
        double div = 0;
        for (int i = 1; i <= 100_000; i++) {
            //the frequency in position i
            div += (Constant / Math.pow(i, skew));
        }

        List<Integer> randomNumList = getRandomNumList(100_000, BOUND);
        double sum = 0;
        for (int i = 1; i <= 100_000; i++) {
            double p = (Constant / Math.pow(i, skew)) / div;
            sum += p;
            map.put(sum, randomNumList.get(i - 1));
        }
        return map;
    }

    public int next() {         // [1,R]
        double value = random.nextDouble();
        return map.ceilingEntry(value).getValue() + 1;
    }

    public List<Integer> getSeparatorNum(int partitionNum) {
        List<Integer> res = new ArrayList<>();
        List<Integer> sampleDatas = new ArrayList<>();
        for (int i = 0; i < sampleSize; i++){
            sampleDatas.add(next());
        }
        Collections.sort(sampleDatas);

        int partitionRange = sampleDatas.size() / partitionNum;
        for(int i = 1; i < partitionNum; i ++) {
            res.add(sampleDatas.get(i * partitionRange) - 1);
        }
        return res;
    }

    public static void main(String[] args) {
        ZipfGenerator zipf = new ZipfGenerator(Consts.TEST_BATCH_KEY_RANGE, 0.3);
        List<Integer> separators = zipf.getSeparatorNum(5);
        System.out.println(separators);

        Map<Integer, Integer> counts = new HashMap<>();
        for(int i = 0; i < 1000; i++){
            int curNum = zipf.next();
            counts.put(curNum, counts.getOrDefault(curNum, 0) + 1);
        }

        List<Integer> keySet = new ArrayList<Integer>(counts.keySet());
        Collections.sort(keySet);
        for(int k: keySet) {
            System.out.println(k + " " + counts.get(k));
        }
    }

}