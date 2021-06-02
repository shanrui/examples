package examples.java.mr;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Stream;
import java.util.stream.IntStream;

class RunMR <K1, V1, K2, V2, K3, V3> {

    @FunctionalInterface
    public interface Mapper<K1, V1, K2, V2> {
        Stream<TwoTuple<K2, V2>> apply(TwoTuple<K1, V1> in);
    }

    @FunctionalInterface
    public interface Reducer<K2, V2, K3, V3> {
        TwoTuple<K3, V3> apply(K2 key2, List<V2> values);
    }

    public void run(Stream<TwoTuple<K1, V1>> input, Mapper<K1, V1, K2, V2> mapper, Reducer<K2, V2, K3, V3> reducer, Comparator<TwoTuple<K2, V2>> comparator){

        Stream<TwoTuple<K2, V2>> results = input
            .flatMap( e -> {
                return mapper.apply(e);
            })
            .sorted( (e1, e2) -> {
                return comparator.compare(e1, e2);
            });

        CurKey<K2> curKey = new CurKey<K2>();
        List<V2> values = new ArrayList<V2>();

        results.forEach(e -> {
            if(!curKey.isInited()){
                curKey.set(e.first);
            }
            if(e.first.equals(curKey.get())){
                values.add(e.second);
            }else{
                TwoTuple<K3, V3> re = reducer.apply(curKey.get(), values);

                System.out.println(re);

                values.clear();
                curKey.set(e.first);
                values.add(e.second);
            }
        });
        TwoTuple<K3, V3> re = reducer.apply(curKey.get(), values);

        System.out.println(re);
    }


    public static void main(String[] argv){
        System.out.println("------ wordCount ------");
        wordCount();
        System.out.println("");
        System.out.println("------ sort ------");
        sort();
        System.out.println("");
        System.out.println("------ wordMean ------");
        wordMean();
    }

    public static void wordCount(){

        RunMR<Integer, String, String, Integer, String, Integer> runMR = new RunMR<Integer, String, String, Integer, String, Integer>();

        List<String> strings = Arrays.asList("h", "abc", "", "bc","h", "efg", "bc", "", "jkl", "abc", "h");

        Stream<TwoTuple<Integer, String>> input = IntStream.range(0, strings.size()).mapToObj( e -> {
            return new TwoTuple(e, strings.get(e));
        });

        runMR.run(
            input,

            in -> {
                return Stream.of(new TwoTuple(in.second, 1));
            },

            (key, values) -> {
                Integer all = values.stream().reduce((cur, acc) -> {
                    return acc + cur;
                }).get();
                return new TwoTuple(key, all);
            },
            
            (o1, o2) -> {
                return o1.first.compareTo(o2.first);
            });
    }

    public static void sort(){

        RunMR<Integer, String, String, Object, String, Object> runMR = new RunMR<Integer, String, String, Object, String, Object>();

        List<String> strings = Arrays.asList("a", "bc", "zxy", "bc2","h", "efg", "bbc", "", "jkl", "abc", "hgh");

        Stream<TwoTuple<Integer, String>> input = IntStream.range(0, strings.size()).mapToObj( e -> {
            return new TwoTuple(e, strings.get(e));
        });

        runMR.run(
            input,

            in -> {
                return Stream.of(new TwoTuple(in.second, null));
            },

            (key, values) -> {
                return new TwoTuple(key, null);
            },
            
            (o1, o2) -> {
                return o1.first.compareTo(o2.first);
            });
    }


    public static void wordMean(){

        RunMR<Integer, String, String, Integer, String, Integer> runMR = new RunMR<Integer, String, String, Integer, String, Integer>();

        List<String> strings = Arrays.asList("abcdfgg", "bcsssssss", "lllzxy", "bc2","h", "efmmmmmg", "bbc", "", "jkkkkkkkkl", "abc", "hgh");

        Stream<TwoTuple<Integer, String>> input = IntStream.range(0, strings.size()).mapToObj( e -> {
            return new TwoTuple(e, strings.get(e));
        });
        
        runMR.run(
            input,

            in -> {
                return Stream.of(new TwoTuple("WORD", 1), new TwoTuple("COUNT", in.second.length()));
            },

            (key, values) -> {
                Integer all = values.stream().reduce((cur, acc) -> {
                    return acc + cur;
                }).get();
                return new TwoTuple(key, all);
            },
            
            (o1, o2) -> {
                return o1.first.compareTo(o2.first);
            });
    }

}


class TwoTuple<A, B> {

    public final A first;

    public final B second;

    public TwoTuple(A a, B b){
        first = a;
        second = b;
    }

    public String toString(){
        return "(" + first + ", " + second + ")";
    }

}

class CurKey<T>{

    public T key;
    Boolean inited = false;

    public CurKey(){
        this.key = null;
    }

    public CurKey(T key){
        this.key = key;
        inited = true;
    }

    public void set(T key){
        this.key = key;
        inited = true;
    }

    public T get(){
        return key;
    }

    public Boolean isInited(){
        return inited;
    }

}   

