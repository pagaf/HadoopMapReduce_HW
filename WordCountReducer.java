package mapreduce;

import java.util.List;

public class WordCountReducer implements Reducer {
    @Override
    public void reduce(String key, List<String> values, ReduceContext context) {
        int sum = 0;
        for (String val : values) {
            try {
                sum += Integer.parseInt(val);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
        context.write(key, String.valueOf(sum));
    }
}
