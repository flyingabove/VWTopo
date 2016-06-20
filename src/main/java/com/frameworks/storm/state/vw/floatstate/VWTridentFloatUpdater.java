package com.frameworks.storm.state.vw.floatstate;

import com.frameworks.storm.comparators.TupleComparator;
import lombok.extern.slf4j.Slf4j;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by christiangao on 6/14/16.
 */
@Slf4j
public class VWTridentFloatUpdater extends BaseStateUpdater<VWTridentFloatState> {
    String tsFieldName;
    String msgName;

    public VWTridentFloatUpdater(String msgName,String tsFieldName){
        this.tsFieldName = tsFieldName;
        this.msgName = msgName;
    }

    public void updateState(VWTridentFloatState state, List<TridentTuple> tuples, TridentCollector collector) {
        Collections.sort(tuples,new TupleComparator(tsFieldName));//sorted list
        List<String> learningMsgList = new ArrayList<String>();
        for(TridentTuple t: tuples) {
            //log.info("TS: "+t.getLongByField(tsFieldName));
            learningMsgList.add(t.getStringByField(msgName));
        }
        state.batchLearn(learningMsgList);

        for (TridentTuple tuple : tuples) {
            collector.emit(tuple);
        }
    }
}
