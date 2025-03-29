package mapreduce;

public interface MapContext {
    void write(String key, String value);
}
