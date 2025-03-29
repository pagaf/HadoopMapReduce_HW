package mapreduce;

public interface ReduceContext {
    void write(String key, String aggregatedValue);
}