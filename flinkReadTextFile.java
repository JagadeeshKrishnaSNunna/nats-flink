import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class flinkReadTextFile {
    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        String path="/home/jadhu/Desktop/inp.txt";
        TextInputFormat format= new TextInputFormat(new Path(path));
//        format.setCharsetName("UTF-8");
        DataStream<String> stream=env.readFile(format,path,FileProcessingMode.PROCESS_CONTINUOUSLY,6000, BasicTypeInfo.STRING_TYPE_INFO);
        DataStream<Tuple2<String,Integer>> data= stream
                .flatMap(new splitter())
                .keyBy(value->value.f0)
                .sum(1);
//        DataStream<Map<String,String>> data= (DataStream<Map<String, String>>) stream
//                .map(new mapConverter())
//                .filter(new FilterFunction<Map<String, String>>() {
//                    @Override
//                    public boolean filter(Map<String, String> stringStringMap) throws Exception {
//                        return stringStringMap.get("message").equals("ola");
//                    }
//                })
//                ;
        data.print();
        env.execute("ola");
    }

    public static class splitter implements FlatMapFunction<String,Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            for(String w : s.split(" ")){
                collector.collect(new Tuple2<String,Integer>(w,1));
            }
        }
    }

    public static class mapConverter implements MapFunction<String, Map<String,String>> {

        @Override
        public Map<String,String> map(String str) throws Exception {
            Map<String, String> reconstructedUtilMap = Arrays.stream(str.substring(1,str.length()-1).split(","))
                    .map(s -> s.split("="))
                    .collect(Collectors.toMap(s -> s[0].trim(), s -> s[1].trim()));
            return reconstructedUtilMap;
        }
    }

}
