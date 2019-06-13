package uk.ac.shef.ischool.wdcindex.app;

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
import java.util.Date;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class NTripleIndexerApp {
    private static final Logger LOG = Logger.getLogger(NTripleIndexerApp.class.getName());

    public static void main(String[] args) throws IOException {

        CoreContainer solrContainer = new CoreContainer(args[2]);
        solrContainer.load();

        SolrClient urlInfo = new EmbeddedSolrServer(solrContainer.getCore("url"));
        //SolrClient predicatesCoreClient= new EmbeddedSolrServer(solrContainer.getCore("predicates"));
        List<String> gzFiles = FileUtils.readLines(new File(args[0]));
        LOG.info("Initialisation completed.");
        NTripleIndexerWorker worker = new NTripleIndexerWorker(0,urlInfo,
                args[1],
                gzFiles);

        try {

            ForkJoinPool forkJoinPool = new ForkJoinPool();
            int total = forkJoinPool.invoke(worker);

            LOG.info(String.format("Completed, total entities=%s", total, new Date().toString()));

        } catch (Exception ioe) {
            StringBuilder sb = new StringBuilder("Failed to build features!");
            sb.append("\n").append(ExceptionUtils.getFullStackTrace(ioe));
            LOG.error(sb.toString());
        }


        urlInfo.close();
        System.exit(0);
    }
}
