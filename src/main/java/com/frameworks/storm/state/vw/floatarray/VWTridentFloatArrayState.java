package com.frameworks.storm.state.vw.floatarray;

import storm.trident.state.State;
import vw.learner.VWFloatLearner;
import vw.learner.VWLearners;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christiangao on 6/14/16.
 */
public class VWTridentFloatArrayState implements State {

    /* This is for numeric type predictions such as regression*/

    VWFloatLearner learner;

    public VWTridentFloatArrayState(String init){

        this.learner = VWLearners.create(init);

    }

    public void beginCommit(Long txid) {
    }

    public void commit(Long txid) {
    }

    public void learn(String learningMsg) {
        learner.learn(learningMsg);
    }

    public void batchLearn(List<String> learningMsgList) {
        for(String learningMsg:learningMsgList) {
            learner.learn(learningMsg);
        }
    }

    public Float predict(String predictionMsg) {
        return learner.predict(predictionMsg);
    }

    public List<Float> batchPredict(List<String> predictionMsgs) {
        List<Float> predictions = new ArrayList<Float>();
        for(String predictionMsg:predictionMsgs) {
            predictions.add(learner.predict(predictionMsg));
        }
        return predictions;
    }

}
