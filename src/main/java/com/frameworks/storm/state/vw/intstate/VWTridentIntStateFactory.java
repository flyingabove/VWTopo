package com.frameworks.storm.state.vw.intstate;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Created by christiangao on 6/15/16.
 */

public class VWTridentIntStateFactory implements StateFactory {

    String init;
    public VWTridentIntStateFactory(String init){
        this.init = init;
    }

    public State makeState(Map conf, IMetricsContext metricsContext,int partitionIndex, int numPartitions) {
        return new VWTridentIntState(init);
    }
}