package examples.java.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;
import java.util.Arrays;
import java.util.List;

public class WordCount 
{
    public static void main( String[] args ) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> lines = Arrays.asList("This is a first sentence", "This is a second sentence with a one word");

        DataSet<String> text = env.fromCollection(lines);

        DataSet<Tuple2<String, Integer>> result = text
            .flatMap(new LineSplitter())
            .groupBy(0)
            .aggregate(Aggregations.SUM, 1);

        List<Tuple2<String, Integer>> collect = result.collect();

        System.out.println(collect);
    }
}

class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        String[] tokens = value.toLowerCase().split("\\W+");
        Stream.of(tokens).filter(t -> t.length() > 0).forEach(token -> out.collect(new Tuple2<>(token, 1)));
    }
}

