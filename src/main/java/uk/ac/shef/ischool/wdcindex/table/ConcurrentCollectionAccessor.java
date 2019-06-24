package uk.ac.shef.ischool.wdcindex.table;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ConcurrentCollectionAccessor {

    public synchronized void increment(String id, Map<String, Integer> freq){
        if (id!=null) {
            if (freq.containsKey(id)) {
                freq.put(id, freq.get(id) + 1);
            } else
                freq.put(id, 1);
        }
    }

    public synchronized void add(String content, Set<String> container){
        if (content!=null)
            container.add(content);
    }

    /*public static void main(String[] args) {
        Map m = new HashMap();
        new ConcurrentCollectionAccessor().increment(null, m);
        System.out.println(m);
    }*/
}
