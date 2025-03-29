package mapreduce;

import java.io.*;
import java.util.*;

public class ReducerThread extends Thread {
    private final int reducerId;
    private final Reducer reducer;
    private final int numMappers;
    private final String intermediateDir;
    private final String outputDir;

    public ReducerThread(int reducerId, Reducer reducer, int numMappers, String intermediateDir, String outputDir) {
        this.reducerId = reducerId;
        this.reducer = reducer;
        this.numMappers = numMappers;
        this.intermediateDir = intermediateDir;
        this.outputDir = outputDir;
    }

    @Override
    public void run() {
        for (int i = 0; i < numMappers; i++) {
            File doneFile = new File(intermediateDir + File.separator + "mapper" + i + ".done");
            while (!doneFile.exists()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        Map<String, List<String>> groupedData = new HashMap<>();
        File dir = new File(intermediateDir);
        File[] files = dir.listFiles((d, name) -> name.matches("mapper\\d+_to_reducer" + reducerId + "\\.txt"));
        if (files != null) {
            for (File file : files) {
                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] parts = line.split("\t");
                        if (parts.length >= 2) {
                            String key = parts[0];
                            String value = parts[1];
                            groupedData.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // Запись результата редьюсинга в выходной файл
        String outFileName = outputDir + File.separator + "reducer" + reducerId + ".txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outFileName))) {
            ReduceContext context = new ReduceContext() {
                @Override
                public void write(String key, String aggregatedValue) {
                    try {
                        writer.write(key + "\t" + aggregatedValue);
                        writer.newLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };

            
            for (Map.Entry<String, List<String>> entry : groupedData.entrySet()) {
                reducer.reduce(entry.getKey(), entry.getValue(), context);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
