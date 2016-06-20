package com.frameworks.storm.state.vw.floatarray;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christiangao on 6/14/16.
 */
public class VWTridentFloatArrayQuery extends BaseQueryFunction<VWTridentFloatArrayState, Float> {

    String msgName;
    public VWTridentFloatArrayQuery(String msgName) {
        this.msgName = msgName;
    }

    public List<Float> batchRetrieve(VWTridentFloatArrayState state, List<TridentTuple> inputs) {
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
