package mapreduce;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class  Main  {
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

        // Динамическая загрузка классов маппера и редьюсера
        Mapper mapper = (Mapper) Class.forName(mapperClassName).getDeclaredConstructor().newInstance();
        Reducer reducer = (Reducer) Class.forName(reducerClassName).getDeclaredConstructor().newInstance();

        // Создаём промежуточную директорию для обмена данными
        String intermediateDir = outputDir + File.separator + "intermediate";
        new File(intermediateDir).mkdirs();

        // Получаем список входных файлов
        File dir = new File(inputDir);
        File[] inputFiles = dir.listFiles();
        if (inputFiles == null) {
            System.err.println("Входная директория пуста или не существует.");
            System.exit(1);
        }

        List<Thread> mapperThreads = new ArrayList<>();
        for (int i = 0; i < inputFiles.length; i++) {
            MapperThread mt = new MapperThread(inputFiles[i], mapper, i, numReducers, intermediateDir);
            mapperThreads.add(mt);
            mt.start();
        }

        List<Thread> reducerThreads = new ArrayList<>();
        for (int i = 0; i < numReducers; i++) {
            ReducerThread rt = new ReducerThread(i, reducer, inputFiles.length, intermediateDir, outputDir);
            reducerThreads.add(rt);
            rt.start();
        }

        for (Thread t : mapperThreads) {
            t.join();
        }
        for (Thread t : reducerThreads) {
            t.join();
        }

        System.out.println("MapReduce обработка завершена.");
    }
}
