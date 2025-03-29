package mapreduce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class FileMapContext implements MapContext {
    private final BufferedWriter writer;

    public FileMapContext(String filePath) throws IOException {
        // Открываем файл для дозаписи
        writer = new BufferedWriter(new FileWriter(filePath, true));
    }

    @Override
    public synchronized void write(String key, String value) {
        try {
            writer.write(key + "\t" + value);
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        writer.close();
    }
}
