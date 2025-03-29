package mapreduce;

public class WordCountMapper implements Mapper {
    @Override
    public void map(String line, MapContext context) {
        String[] words = line.split("\\s+");
        for (String word : words) {
            if (!word.isEmpty()) {
                context.write(word, "1");
            }
        }
    }
}
