package com.frameworks.storm.operation;

import backtype.storm.tuple.Values;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

@Slf4j
public class VWPredictFieldGenerator extends BaseFunction{

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {

        collector.emit(new Values("age:2 gender:3 religion:4"));

    }
}
