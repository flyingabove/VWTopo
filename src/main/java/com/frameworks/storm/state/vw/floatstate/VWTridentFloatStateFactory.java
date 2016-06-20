package com.frameworks.storm.state.vw.floatstate;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by christiangao on 6/15/16.
 */

public class VWTridentFloatStateFactory implements StateFactory {

    String init;
    public VWTridentFloatStateFactory(String init){
        this.init = init;
    }

    public State makeState(Map conf, IMetricsContext metricsContext,int partitionIndex, int numPartitions) {
        return new VWTridentFloatState(init);
    }
}