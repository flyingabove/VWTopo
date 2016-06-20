package com.frameworks.storm.operation;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

@Slf4j
public class VWPredictFieldGenerator extends BaseFunction {
    String fileName;
    int batchSize = 100;

    public VWPredictFieldGenerator(String fileName) {
        this.fileName = fileName;
    }

    public long getTimestamp() {
        return System.nanoTime();
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String line;

        try {
            FileReader fileReader = new FileReader(fileName);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            for (int i = 1; (i <= batchSize); i++) {
                final long timestamp = getTimestamp();
                line = bufferedReader.readLine();
                if (line == null) {
                    break;
                }
                log.info("Emitting {} with timestamp {}", line, timestamp);
                collector.emit(new Values(line, timestamp));
            }
            return;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
