package uk.ac.shef.ischool.wdcindex;

import org.apache.solr.client.solrj.SolrClient;

import java.util.*;
import java.util.concurrent.RecursiveTask;

public abstract class Worker extends RecursiveTask<Integer> {
    protected int id;
    protected SolrClient solrClient;
    protected List<String> jobs;
    protected int maxJobsPerThread;



    public Worker(int id, SolrClient solrClient,
                               List<String> jobs) {
        this.id=id;
        this.solrClient = solrClient;
        this.jobs = jobs;
    }

    public void setMaxJobsPerThread(int jobs){
        this.maxJobsPerThread=jobs;
    }


    @Override
    protected Integer compute() {
        if (this.jobs.size() > maxJobsPerThread) {
            List<Worker> subWorkers =
                    new ArrayList<>(createSubWorkers());
            for (Worker subWorker : subWorkers)
                subWorker.fork();
            return mergeResult(subWorkers);
        } else {
            return computeSingleWorker(jobs);
        }
    }


    /**
     * Query the solr backend to process tweets
     *
     * @param
     * @return
     */
    protected abstract int computeSingleWorker(List<String> jobs);


    protected List<Worker> createSubWorkers() {
        List<Worker> subWorkers =
                new ArrayList<>();

        boolean b = false;
        List<String> splitTask1 = new ArrayList<>();
        List<String> splitTask2 = new ArrayList<>();
        for (String e : jobs) {
            if (b)
                splitTask1.add(e);
            else
                splitTask2.add(e);
            b = !b;
        }

        Worker subWorker1 = createInstance(splitTask1, this.id+1);
        Worker subWorker2 = createInstance(splitTask2, this.id+2);

        subWorkers.add(subWorker1);
        subWorkers.add(subWorker2);

        return subWorkers;
    }

    /**
     * NOTE: classes implementing this method must call setHashtagMap and setMaxPerThread after creating your object!!
     * @param id
     * @return
     */
    protected abstract Worker createInstance(List<String> jobs, int id);
    /*{
        return new IndexAnalyserWorker(id, this.solrClient, splitTasks, maxTasksPerThread, outFolder);
    }*/

    protected int mergeResult(List<Worker> workers) {
        Integer total = 0;
        for (Worker worker : workers) {
            total += worker.join();
        }
        return total;
    }

}
