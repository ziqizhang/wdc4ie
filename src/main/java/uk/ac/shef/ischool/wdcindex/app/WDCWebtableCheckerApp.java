package uk.ac.shef.ischool.wdcindex.app;

import com.google.common.collect.Lists;
import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import uk.ac.shef.ischool.wdcindex.table.ConcurrentCollectionAccessor;
import uk.ac.shef.ischool.wdcindex.table.WDCWebtableOverlapChecker;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WDCWebtableCheckerApp {
    private static final Logger LOG = Logger.getLogger(WDCWebtableCheckerApp.class.getName());

    private static void exportData(String outFolder, int maxLines,
                                   Map<String, Integer> hostEntityTables,
                                   Map<String, Integer> hostRelationTables,
                                   Set<String> urlEntityTables,
                                   Set<String> urlRelationTables) throws IOException {
        LOG.info("Exporting host-entitytable count...");
        exportMap(outFolder, "host-entitytable", maxLines, hostEntityTables);
        LOG.info("Exporting host-relationtable count...");
        exportMap(outFolder, "host-relationtable", maxLines, hostRelationTables);
        LOG.info("Exporting url-entitytable list...");
        exportSet(outFolder, "url-entitytable", maxLines, urlEntityTables);
        LOG.info("Exporting url-relationtable list...");
        exportSet(outFolder, "url-relationtable", maxLines, urlRelationTables);
    }

    private static void exportMap(String outFolder, String filePrefix, int maxLines,
                                  Map<String, Integer> map) throws IOException {
        int fileCount=1;
        int lines=0;
        CSVWriter writer =
                new CSVWriter(new FileWriter(outFolder+"/"+filePrefix+"_"+fileCount+".csv"));

        LOG.info("\t total="+map.size());
        for(Map.Entry<String, Integer> e : map.entrySet()){
            lines++;
            String[] values = {e.getKey(), String.valueOf(e.getValue())};
            writer.writeNext(values);

            if(lines>=maxLines){
                fileCount++;
                writer.close();
                writer=new CSVWriter(new FileWriter(outFolder+"/"+filePrefix+"_"+fileCount+".csv"));
                lines=0;
            }
        }

        writer.close();
    }

    private static void exportSet(String outFolder, String filePrefix, int maxLines,
                                  Set<String> set) throws IOException {
        int fileCount=1;
        int lines=0;
        PrintWriter writer = new PrintWriter(outFolder+"/"+filePrefix+"_"+fileCount+".txt");

        LOG.info("\t total="+set.size());
        for(String l : set){
            lines++;

            writer.println(l);

            if(lines>=maxLines){
                fileCount++;
                writer.close();
                writer=new PrintWriter(outFolder+"/"+filePrefix+"_"+fileCount+".txt");
                lines=0;
            }
        }

        writer.close();
    }

    public static void main(String[] args) throws IOException {
        String hostRootURL = args[4];
        //"http://data.dws.informatik.uni-mannheim.de/webtables/2015-07/completeCorpus/compressed/";
        SolrClient urlCache = null;
        CoreContainer solrContainer = new CoreContainer(args[1]);
        solrContainer.load();
        urlCache = new EmbeddedSolrServer(solrContainer.getCore("url"));


        List<String> files = FileUtils.readLines(new File(args[0]), Charset.forName("utf8"));
        List<String> gzFiles = new ArrayList<>(files.size());
        for (String f : files)
            if (f.endsWith("gz"))
                gzFiles.add(hostRootURL + f);
        Collections.sort(gzFiles);
        int size = Integer.valueOf(args[2]);
        List<List<String>> parts = Lists.partition(gzFiles, size);

        DB db = DBMaker.fileDB(args[3] + "/WDCWebtableOverlap.db")
                .fileMmapEnable()
                .allocateStartSize(1 * 1024 * 1024 * 1024)  // 10GB
                .allocateIncrement(512 * 1024 * 1024)       // 512MB
                .make();

        Map<String, Integer> relationTableSourceDomain
                = db.hashMap("REL-host", Serializer.STRING, Serializer.INTEGER).createOrOpen();
        Map<String, Integer> entityTableSourceDomain
                = db.hashMap("ENT-host", Serializer.STRING, Serializer.INTEGER).createOrOpen();
        Set<String> relationTableSourceURL
                = db.hashSet("REL-url", Serializer.STRING).createOrOpen();
        Set<String> entityTableSourceURL
                = db.hashSet("ENT-url", Serializer.STRING).createOrOpen();
        ConcurrentCollectionAccessor cca = new ConcurrentCollectionAccessor();

        LOG.info("Initialisation completed.");

        ExecutorService executor = Executors.newFixedThreadPool(parts.size());
        /*
        int id, int batch, String outFolder,
                                     List<String> gzFiles, SolrClient client,
                                     Map<String, Integer> hostEntityTables,
                                     Map<String, Integer> hostRelationTables,
                                     ConcurrentCollectionAccessor cca
         */
        for (int i = 0; i < parts.size(); i++) {
            Runnable worker = new WDCWebtableOverlapChecker(i,
                    5000, args[3],
                    parts.get(i), urlCache,
                    entityTableSourceDomain,
                    relationTableSourceDomain,
                    entityTableSourceURL,
                    relationTableSourceURL,
                    cca
            );
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        LOG.info(String.format("Completed all threads", new Date().toString()));

        urlCache.close();

        //exportData();
        db.close();
        System.exit(0);
    }
}
