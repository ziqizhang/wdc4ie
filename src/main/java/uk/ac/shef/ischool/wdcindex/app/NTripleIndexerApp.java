package uk.ac.shef.ischool.wdcindex.app;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import uk.ac.shef.ischool.wdcindex.pcd.NTripleIndexerWorker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

public class NTripleIndexerApp {
    private static final Logger LOG = Logger.getLogger(NTripleIndexerApp.class.getName());

    public static void main(String[] args) throws IOException {

        CoreContainer solrContainer = new CoreContainer(args[2]);
        solrContainer.load();

        SolrClient urlInfo = new EmbeddedSolrServer(solrContainer.getCore("url"));
        //SolrClient predicatesCoreClient= new EmbeddedSolrServer(solrContainer.getCore("predicates"));
        List<String> gzFiles = FileUtils.readLines(new File(args[0]));
        Collections.sort(gzFiles);
        int size = Integer.valueOf(args[4]);
        List<List<String>> parts=Lists.partition(gzFiles, size);


        LOG.info("Initialisation completed.");

        ExecutorService executor = Executors.newFixedThreadPool(parts.size());
        for (int i = 0; i < parts.size(); i++) {
            Runnable worker = new NTripleIndexerWorker(i,urlInfo,
                    args[1],
                    parts.get(i), args[3]);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        LOG.info(String.format("Completed all threads", new Date().toString()));

        urlInfo.close();
        System.exit(0);
    }
}
