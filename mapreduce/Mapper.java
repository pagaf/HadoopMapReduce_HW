package mapreduce;

public interface Mapper {
    void map(String line, MapContext context);
}
