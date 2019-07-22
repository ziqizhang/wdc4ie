package uk.ac.shef.ischool.wdcindex.app;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import uk.ac.shef.ischool.wdcindex.pcd.NTripleIndexerWorker_NoCC;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
/home/zz/Work/wdc4ie/resources/solr_wdc
 */
public class NTripleIndexerApp_NoCC {
    private static final Logger LOG = Logger.getLogger(NTripleIndexerApp_NoCC.class.getName());

    public static void main(String[] args) throws IOException {

        /*String content="http://ok.com> http://ok.com> \"values\" .";
        Pattern p = Pattern.compile("\\<(.*?)\\>");
        Matcher m = p.matcher(content);

        int prevE=0;
        String composed="";
        while(m.find()){
            int s = m.start();
            int e = m.end();
            String v = m.group();
            v=v.substring(1, v.length()-1).trim();

            //before the match
            composed+=content.substring(prevE, s);
            //the match
            composed+="<"+v+">";

            prevE=e;

            System.out.println();
        }
        composed+=content.substring(prevE);
        System.exit(1);
*/
        SolrClient urlCache=null;
        boolean indexURL = args.length==5;
        if (indexURL) {
            CoreContainer solrContainer = new CoreContainer(args[3]);
            solrContainer.load();
            urlCache = new EmbeddedSolrServer(solrContainer.getCore("url"));
        }
        System.out.println("Use solr="+urlCache);

        String tmpFolder = args[1] + "/tmp";
        File tmpStore = new File(tmpFolder);
        Set<String> processedJobs = new HashSet<>();
        if (!tmpStore.exists())
            tmpStore.mkdirs();
        else {
            for (File f : tmpStore.listFiles()) {
                if (f.getName().endsWith(".job")) {
                    List<String> processed=FileUtils.readLines(f, Charset.forName("utf-8"));
                    LOG.info("loaded "+processed.size()+" already processed from "+f);
                    for (String pf : processed)
                        processedJobs.add(pf.split(",")[0]);
                }
            }
            LOG.info("Total already processed files:" + processedJobs.size());
        }

        List<String> gzFiles = FileUtils.readLines(new File(args[0]));
        gzFiles.removeAll(processedJobs);
        LOG.info("Total files to process:" + gzFiles.size());
        Collections.sort(gzFiles);
        int size = Integer.valueOf(args[3]);
        List<List<String>> parts=
                Lists.partition(gzFiles, size);
        LOG.info("Total threads to be created:" + parts.size());


        LOG.info("Initialisation completed.");

        ExecutorService executor = Executors.newFixedThreadPool(parts.size());
        for (int i = 0; i < parts.size(); i++) {
            Runnable worker = new NTripleIndexerWorker_NoCC(i,Integer.valueOf(args[2]),
                    args[1],
                    parts.get(i), urlCache);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        LOG.info(String.format("Completed all threads", new Date().toString()));

        if (indexURL)
            urlCache.close();
        System.exit(0);
    }
}
