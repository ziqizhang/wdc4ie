package uk.ac.shef.ischool.wdcindex.table;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * for each webtable in the 2015 july webtable corpus: http://data.dws.informatik.uni-mannheim.de/webtables/2015-07/completeCorpus/compressed/
 * find
 * - if the source url is also found in the wdc triple dataset released in Nov 2015
 * - if yes, output url and the source WDC gz file
 * <p>
 * - create inverted map of host-table count
 * <p>
 * Relation and Entity table should be treated separately
 * <p>
 * todo: code to load NTripleIndexer output csv files
 * todo: code to use CC index to find offset of url
 */
public class WDCWebtableOverlapChecker implements Runnable {
    private SolrClient urlCore;
    private int commitBatch;
    private int id;

    private String outFolder;

    private static final Logger LOG = Logger.getLogger(WDCWebtableOverlapChecker.class.getName());

    private List<String> gzFiles;
    private Map<String, Integer> hostEntityTables;
    private Map<String, Integer> hostRelationTables;
    private Set<String> urlEntityTables;
    private Set<String> urlRelationTables;
    private ConcurrentCollectionAccessor cca;
    private Gson gson = new Gson();

    public WDCWebtableOverlapChecker(int id, int batch, String outFolder,
                                     List<String> gzFiles, SolrClient client,
                                     Map<String, Integer> hostEntityTables,
                                     Map<String, Integer> hostRelationTables,
                                     Set<String> urlEntityTables,
                                     Set<String> urlRelationTables,
                                     ConcurrentCollectionAccessor cca) {
        this.id=id;
        this.commitBatch=batch;
        this.outFolder=outFolder;
        this.gzFiles=gzFiles;
        this.hostEntityTables=hostEntityTables;
        this.hostRelationTables=hostRelationTables;
        this.urlEntityTables=urlEntityTables;
        this.urlRelationTables=urlRelationTables;
        this.cca=cca;
        this.urlCore=client;
    }

    @Override
    public void run() {
        int countFiles = 0;
        for (String inputGZFile : gzFiles) {
            countFiles++;
            try {
                LOG.info("THREAD " + id + " Processing " + countFiles + "/" + gzFiles.size() + ", " + inputGZFile);
                LOG.info("\t thread " + id + " downloading..." + inputGZFile);
                URL downloadFrom = new URL(inputGZFile);
                File downloadTo = new File(this.outFolder + "/" + new File(downloadFrom.getPath()).getName());
                FileUtils.copyURLToFile(downloadFrom, downloadTo);

                BufferedReader in = new BufferedReader(new InputStreamReader(
                        new GZIPInputStream(new FileInputStream(downloadTo)), Charset.forName("utf8")));

                String content;
                int countLines=0;
                while ((content = in.readLine()) != null) {
                    //parse json string
                    try {
                        Map<String, Object> values = gson.fromJson(content,
                                new TypeToken<HashMap<String, Object>>() {
                                }.getType());
                        String tableType = values.get("tableType").toString();
                        String url = values.get("url").toString();
                        URL urlObj = new URL(url);
                        String domain = urlObj.getHost();

                        SolrQuery query = createQuery(url);
                        QueryResponse res = urlCore.query(query);
                        if (res != null && res.getResults().getNumFound()>0) {
                            if (tableType.equalsIgnoreCase("entity")) {
                                cca.increment(domain, hostEntityTables);
                                cca.add(url, urlEntityTables);
                            } else if (tableType.equalsIgnoreCase("relation")) {
                                cca.increment(domain, hostRelationTables);
                                cca.add(url, urlRelationTables);
                            }
                        }
                    }catch (Exception e){
                        LOG.warn(String.format("\t\tthread " + id + " encountered problem for GZ %s: at line %d, due to %s",
                                inputGZFile, countLines, ExceptionUtils.getFullStackTrace(e)));
                    }
                    countLines++;
                    if (countLines%commitBatch==0)
                        LOG.info(String.format("\t\tthread " + id + " completed %d lines for GZ %s",
                                countLines, inputGZFile));
                    LOG.info(String.format("\t\t\tstats:(t=" + id + ") %d hosts with entity tables, %d hosts with relation tables" +
                                    " %d urls with entity tables, %d urls with relation tables",
                            hostEntityTables.size(), hostRelationTables.size(),
                            urlEntityTables.size(), urlRelationTables.size()));
                }
                LOG.info(String.format("\t\tthread " + id + " completed GZ %s, with total lines=%d",
                        inputGZFile,countLines));

            } catch (Exception e) {
                e.printStackTrace();
                LOG.warn(String.format("\t\tThread " + id + " encountered problem for GZ: %s, due to %s",
                        inputGZFile, ExceptionUtils.getFullStackTrace(e)));
            }
        }
    }

    private SolrQuery createQuery(String url){
        SolrQuery query = new SolrQuery();
        query.setQuery("id:"+url);
        //query.setSort("random_1234", SolrQuery.ORDER.asc);
        return query;
    }

}
