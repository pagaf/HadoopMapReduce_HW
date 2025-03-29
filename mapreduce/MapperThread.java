package mapreduce;

import java.io.*;

public class MapperThread extends Thread {
    private final File inputFile;
    private final Mapper mapper;
    private final int mapperId;
    private final int numReducers;
    private final String intermediateDir;

    public MapperThread(File inputFile, Mapper mapper, int mapperId, int numReducers, String intermediateDir) {
        this.inputFile = inputFile;
        this.mapper = mapper;
        this.mapperId = mapperId;
        this.numReducers = numReducers;
        this.intermediateDir = intermediateDir;
    }

    @Override
    public void run() {
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            // Создаём контексты для каждой редьюсер-партиции
            FileMapContext[] contexts = new FileMapContext[numReducers];
            for (int i = 0; i < numReducers; i++) {
                String fileName = intermediateDir + File.separator + "mapper" + mapperId + "_to_reducer" + i + ".txt";
                contexts[i] = new FileMapContext(fileName);
            }

            String line;
            while ((line = br.readLine()) != null) {
                mapper.map(line, new MapContext() {
                    @Override
                    public void write(String key, String value) {
                        int partition = Math.abs(key.hashCode()) % numReducers;
                        contexts[partition].write(key, value);
                    }
                });
            }

            // Закрываем все контексты (файлы)
            for (FileMapContext ctx : contexts) {
                ctx.close();
            }
            File doneFile = new File(intermediateDir + File.separator + "mapper" + mapperId + ".done");
            doneFile.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
