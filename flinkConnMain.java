import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class flinkConnMain {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.addSource(new natsConnector());
        DataStream<Tuple2<String,Integer>> data= dataStream
                .flatMap(new flinkReadTextFile.splitter())
                .keyBy(value->value.f0)
                .sum(1);

        data.print();

        env.execute("Window WordCount");
    }
    public static class splitter implements FlatMapFunction<String,Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for(String w : s.split(" ")){
                collector.collect(new Tuple2<String,Integer>(w,1));
            }
        }
    }

}
