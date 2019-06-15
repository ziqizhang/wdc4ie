package uk.ac.shef.ischool.wdcindex.app;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import uk.ac.shef.ischool.wdcindex.pcd.NTripleIndexerWorker;
import uk.ac.shef.ischool.wdcindex.pcd.NTripleIndexerWorker_NoSolr;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NTripleIndexerApp_NoSolr {
    private static final Logger LOG = Logger.getLogger(NTripleIndexerApp_NoSolr.class.getName());

    public static void main(String[] args) throws IOException {

        //SolrClient predicatesCoreClient= new EmbeddedSolrServer(solrContainer.getCore("predicates"));
        List<String> gzFiles = FileUtils.readLines(new File(args[0]));
        Collections.sort(gzFiles);
        int size = Integer.valueOf(args[3]);
        List<List<String>> parts=Lists.partition(gzFiles, size);


        LOG.info("Initialisation completed.");

        ExecutorService executor = Executors.newFixedThreadPool(parts.size());
        for (int i = 0; i < parts.size(); i++) {
            Runnable worker = new NTripleIndexerWorker_NoSolr(i,
                    args[1],
                    parts.get(i), args[2]);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        LOG.info(String.format("Completed all threads", new Date().toString()));

        System.exit(0);
    }
}
