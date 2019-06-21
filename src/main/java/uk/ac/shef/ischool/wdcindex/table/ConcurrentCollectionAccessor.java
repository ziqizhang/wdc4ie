package uk.ac.shef.ischool.wdcindex.table;

import java.util.Map;
import java.util.Set;

public class ConcurrentCollectionAccessor {

    public synchronized void increment(String id, Map<String, Integer> freq){
        if (freq.containsKey(id)){
            freq.put(id, freq.get(id)+1);
        }
        else
            freq.put(id, 1);
    }

    public synchronized void add(String content, Set<String> container){
        container.add(content);
    }
}
