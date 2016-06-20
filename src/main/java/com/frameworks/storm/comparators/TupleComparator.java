package com.frameworks.storm.comparators;
import storm.trident.tuple.TridentTuple;

import java.util.Comparator;

/**
 * Created by christiangao on 6/9/16.
 */
public class TupleComparator implements Comparator<TridentTuple> {

    String tsName;

    public TupleComparator(String tsName){
        this.tsName = tsName;
    }

    @Override
    public int compare(TridentTuple o1, TridentTuple o2) {

        long bp1 = (long) o1.getValueByField(tsName);
        long bp2 = (long) o2.getValueByField(tsName);


        if(bp1 < bp2){
            return -1;
        }
        else if(bp1 == bp2){
            return 0;
        }
        else {
            return 1;
        }
    }
}