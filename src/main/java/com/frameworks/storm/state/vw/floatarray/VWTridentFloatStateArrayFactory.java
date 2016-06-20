package com.frameworks.storm.state.vw.floatarray;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by christiangao on 6/15/16.
 */

public class VWTridentFloatStateArrayFactory implements StateFactory {

    String init;
    public VWTridentFloatStateArrayFactory(String init){
        this.init = init;
    }

    public State makeState(Map conf, IMetricsContext metricsContext,int partitionIndex, int numPartitions) {
        return new VWTridentFloatArrayState(init);
    }
}