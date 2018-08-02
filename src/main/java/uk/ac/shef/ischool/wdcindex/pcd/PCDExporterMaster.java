package uk.ac.shef.ischool.wdcindex.pcd;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ForkJoinPool;

public class PCDExporterMaster {

    private static final Logger LOG = Logger.getLogger(PCDExporterMaster.class.getName());

    private List<String> jobs;
    private int threads=1;
    private SolrClient solrClient;
    private String outFolder;

    public PCDExporterMaster(SolrClient solrClient, String fileList, String outFolder) throws IOException {
        this.jobs= FileUtils.readLines(new File(fileList), Charset.forName("utf-8"));
        this.solrClient=solrClient;
        this.outFolder=outFolder;
    }

    public void process() {
        try {
            PCDExporterWorker worker = new PCDExporterWorker(0, this.solrClient,
                    null, this.jobs,this.outFolder);
            int maxPerThread = jobs.size() / threads;
            worker.setMaxJobsPerThread(maxPerThread);

            LOG.info(String.format("Beginning processing %d files on %d threads, at %s", jobs.size(), threads,
                    new Date().toString()));

            ForkJoinPool forkJoinPool = new ForkJoinPool(maxPerThread);
            int total = forkJoinPool.invoke(worker);

            LOG.info(String.format("Completed %d hashtags at %s", total, new Date().toString()));

        } catch (Exception ioe) {
            StringBuilder sb = new StringBuilder("Failed to build features!");
            sb.append("\n").append(ExceptionUtils.getFullStackTrace(ioe));
            LOG.error(sb.toString());
        }

    }

    public void setThreads(int threads) {
        this.threads = threads;
    }
}
