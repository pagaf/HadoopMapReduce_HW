import java.io.*;
import java.util.*;
import java.lang.reflect.*;

interface Mapper {
    
    void map(String line, MapContext context);
}

interface Reducer {
    void reduce(String key, List<String> values, ReduceContext context);
}

interface MapContext {
    void write(String key, String value);
}

interface ReduceContext {
    void write(String key, String aggregatedValue);
}

class FileMapContext implements MapContext {
    private final BufferedWriter writer;

    public FileMapContext(String filePath) throws IOException {
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


class SortMapper implements Mapper {
    @Override
    public void map(String line, MapContext context) {
       
        context.write(line, line);
    }
}

// Пример реализации редьюсера для сортировки – сортирует полученные строки и выводит их
class SortReducer implements Reducer {
    @Override
    public void reduce(String key, List<String> values, ReduceContext context) {
      
        context.write(key, "");
    }
}

class MapperThread extends Thread {
    private final File inputFile;
    private final String mapperClassName; 
    private final int mapperId;
    private final String intermediateDir;

    private final String mapperOutFile;

    private final List<String> sampleKeys = new ArrayList<>();

    public MapperThread(File inputFile, String mapperClassName, int mapperId, String intermediateDir) {
        this.inputFile = inputFile;
        this.mapperClassName = mapperClassName;
        this.mapperId = mapperId;
        this.intermediateDir = intermediateDir;
        this.mapperOutFile = intermediateDir + File.separator + "mapper" + mapperId + ".out";
    }

    @Override
    public void run() {
        try {
           
            Mapper mapper = (Mapper) Class.forName(mapperClassName).getDeclaredConstructor().newInstance();
        
            BufferedWriter outWriter = new BufferedWriter(new FileWriter(mapperOutFile, true));

          
            MapContext context = new MapContext() {
                @Override
                public synchronized void write(String key, String value) {
                    try {
            
                        outWriter.write(key + "\t" + value);
                        outWriter.newLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };

            BufferedReader br = new BufferedReader(new FileReader(inputFile));
            String line;
            int lineCount = 0;
            while ((line = br.readLine()) != null) {
                mapper.map(line, context);
               
                if (lineCount % 10 == 0) {
                    sampleKeys.add(line);
                }
                lineCount++;
            }
            br.close();
            outWriter.close();
            String sampleFile = intermediateDir + File.separator + "mapper" + mapperId + ".sample";
            BufferedWriter sampleWriter = new BufferedWriter(new FileWriter(sampleFile, true));
            for (String sample : sampleKeys) {
                sampleWriter.write(sample);
                sampleWriter.newLine();
            }
            sampleWriter.close();
        
            new File(intermediateDir + File.separator + "mapper" + mapperId + ".done").createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class ReducerThread extends Thread {
    private final int reducerId;
    private final String reducerClassName; 
    private final String intermediateDir;
    private final String outputDir;
    private final String partitionFile; 

    public ReducerThread(int reducerId, String reducerClassName, String intermediateDir, String outputDir) {
        this.reducerId = reducerId;
        this.reducerClassName = reducerClassName;
        this.intermediateDir = intermediateDir;
        this.outputDir = outputDir;
        this.partitionFile = intermediateDir + File.separator + "reducer" + reducerId + ".intermediate";
    }

    @Override
    public void run() {
        try {
     
            Reducer reducer = (Reducer) Class.forName(reducerClassName).getDeclaredConstructor().newInstance();

            File partFile = new File(partitionFile);
            if (!partFile.exists()) {
                System.err.println("Файл " + partitionFile + " не найден.");
                return;
            }
            BufferedReader br = new BufferedReader(new FileReader(partitionFile));
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = br.readLine()) != null) {
                
                String[] parts = line.split("\t", 2);
                if (parts.length >= 1) {
                    lines.add(parts[0]); 
                }
            }
            br.close();

          
            Collections.sort(lines);

        
            String outFileName = outputDir + File.separator + "reducer" + reducerId + ".txt";
            BufferedWriter writer = new BufferedWriter(new FileWriter(outFileName));

            ReduceContext context = new ReduceContext() {
                @Override
                public void write(String key, String aggregatedValue) {
                    try {
                        writer.write(key + (aggregatedValue.isEmpty() ? "" : "\t" + aggregatedValue));
                        writer.newLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            };

            for (String key : lines) {
                
                reducer.reduce(key, Arrays.asList(key), context);
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

public class MapReduceSort {
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: <inputDir> <outputDir> <numReducers> <mapperClass> <reducerClass>");
            System.exit(1);
        }
        String inputDir = args[0];
        String outputDir = args[1];
        int numReducers = Integer.parseInt(args[2]);
        String mapperClassName = args[3];
        String reducerClassName = args[4];

        // Создаем промежуточную директорию
        String intermediateDir = outputDir + File.separator + "intermediate";
        new File(intermediateDir).mkdirs();

        File inDir = new File(inputDir);
        File[] inputFiles = inDir.listFiles();
        if (inputFiles == null || inputFiles.length == 0) {
            System.err.println("Входная директория пуста или не существует.");
            System.exit(1);
        }
        List<Thread> mapperThreads = new ArrayList<>();
        for (int i = 0; i < inputFiles.length; i++) {
            MapperThread mt = new MapperThread(inputFiles[i], mapperClassName, i, intermediateDir);
            mapperThreads.add(mt);
            mt.start();
        }
        for (Thread t : mapperThreads) {
            t.join();
        }


        List<String> allSamples = new ArrayList<>();
        for (int i = 0; i < inputFiles.length; i++) {
            String sampleFile = intermediateDir + File.separator + "mapper" + i + ".sample";
            File f = new File(sampleFile);
            if (f.exists()) {
                BufferedReader br = new BufferedReader(new FileReader(f));
                String s;
                while ((s = br.readLine()) != null) {
                    allSamples.add(s);
                }
                br.close();
            }
        }
        Collections.sort(allSamples);
        String[] boundaries = new String[numReducers - 1];
        if (!allSamples.isEmpty() && numReducers > 1) {
            for (int i = 1; i < numReducers; i++) {
                int index = (int) (((double) i / numReducers) * allSamples.size());
          
                if (index >= allSamples.size()) {
                    index = allSamples.size() - 1;
                }
                boundaries[i - 1] = allSamples.get(index);
            }
        }
        System.out.println("Вычисленные границы партиций: " + Arrays.toString(boundaries));

        BufferedWriter[] partitionWriters = new BufferedWriter[numReducers];
        for (int i = 0; i < numReducers; i++) {
            String partFile = intermediateDir + File.separator + "reducer" + i + ".intermediate";
            partitionWriters[i] = new BufferedWriter(new FileWriter(partFile, true));
        }
        // Проходим по каждому маппер-выходу
        for (int i = 0; i < inputFiles.length; i++) {
            String mapperOut = intermediateDir + File.separator + "mapper" + i + ".out";
            File mapperOutFile = new File(mapperOut);
            if (!mapperOutFile.exists()) continue;
            BufferedReader br = new BufferedReader(new FileReader(mapperOutFile));
            String line;
            while ((line = br.readLine()) != null) {
                
                String[] parts = line.split("\t", 2);
                String key = parts[0];
                int partition = getPartition(key, boundaries, numReducers);
                partitionWriters[partition].write(line);
                partitionWriters[partition].newLine();
            }
            br.close();
        }
        for (int i = 0; i < numReducers; i++) {
            partitionWriters[i].close();
        }

        List<Thread> reducerThreads = new ArrayList<>();
        for (int i = 0; i < numReducers; i++) {
            ReducerThread rt = new ReducerThread(i, reducerClassName, intermediateDir, outputDir);
            reducerThreads.add(rt);
            rt.start();
        }
        for (Thread t : reducerThreads) {
            t.join();
        }

        System.out.println("Распределённая сортировка завершена.");
    }
    private static int getPartition(String key, String[] boundaries, int numReducers) {
        if (boundaries == null || boundaries.length == 0) {
            return 0;
        }
        for (int i = 0; i < boundaries.length; i++) {
            if (key.compareTo(boundaries[i]) < 0) {
                return i;
            }
        }
        return numReducers - 1;
    }
}
