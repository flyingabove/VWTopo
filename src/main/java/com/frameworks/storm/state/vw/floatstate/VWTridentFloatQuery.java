package com.frameworks.storm.state.vw.floatstate;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import storm.trident.state.BaseQueryFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christiangao on 6/14/16.
 */
public class VWTridentFloatQuery extends BaseQueryFunction<VWTridentFloatState, Float> {

    String msgName;
    public VWTridentFloatQuery(String msgName) {
        this.msgName = msgName;
    }

    public List<Float> batchRetrieve(VWTridentFloatState state, List<TridentTuple> inputs) {
        List<String> msgList = new ArrayList<String>();
        for(TridentTuple input: inputs) {
            msgList.add(input.getStringByField(msgName));
        }
        return state.batchPredict(msgList);
    }

    public void execute(TridentTuple tuple, Float prediction, TridentCollector collector) {
        collector.emit(new Values(prediction));
    }
}
