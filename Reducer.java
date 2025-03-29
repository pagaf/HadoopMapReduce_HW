package mapreduce;

import java.util.List;

public interface Reducer {
    void reduce(String key, List<String> values, ReduceContext context);
}
