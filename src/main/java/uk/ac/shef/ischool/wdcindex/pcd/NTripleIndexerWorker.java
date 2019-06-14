package uk.ac.shef.ischool.wdcindex.pcd;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.RecursiveTask;
import java.util.zip.GZIPInputStream;

/**
 * this file reads lines of n-quads and index them accordingly to 'entities' and 'predicates' indexes
 * <p>
 * WARNING: this assumes that same entities do not appear twice in the source data! If that's not the case, data indexed
 * may not be complete
 * <p>
 * WARNING: you need to ensure your data are thread-safe, that is, when different parts of data are processed concurrently
 * by different threads, there will not be identical data instances written by different threads
 *
 *
 *
 * /home/zz/Work/wdc4ie/resources/WDC2017-file.list
 * /home/zz/Work/wdc4ie/resources/WDCTest-file.list.txt
 /home/zz/Work/wdc4ie/resources/output
 /home/zz/Work/wdc4ie/resources/solr_wdc
 http://localhost:8080/CC-MAIN-2017-47-index
 */

public class NTripleIndexerWorker extends RecursiveTask<Integer> {
    private SolrClient urlInfo;
    //private SolrClient predicatesCoreClient;
    private int commitBatch = 5000;
    private int id;

    private String outFolder;
    private String ccIndexURL;

    private static final Logger LOG = Logger.getLogger(NTripleIndexerWorker.class.getName());

    private DB db;
    private int maxTasksPerThread = 5000;
    private List<String> gzFiles;
    private Map<String, String> urlCache= null;



    public NTripleIndexerWorker(int id,
                                SolrClient urlInfo, String outFolder,
                                List<String> inputGZFiles, String ccIndexURL) {
        this.id = id;
        this.urlInfo = urlInfo;
        //this.predicatesCoreClient = predicatesCoreClient;

        this.gzFiles = inputGZFiles;
        this.outFolder = outFolder;
        this.ccIndexURL=ccIndexURL;
    }

    private Scanner setScanner(String file) throws IOException {
        InputStream fileStream = new FileInputStream(file);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, Charset.forName("utf8"));
        Scanner inputScanner = new Scanner(decoder);
        inputScanner.useDelimiter(" .");
        LOG.info("\t Thread " + id + " Obtained scanner object in put file");
        return inputScanner;
    }

    protected int computeSingleWorker(List<String> gzFiles) throws IOException {
        for (String inputGZFile : gzFiles) {
            db = DBMaker.fileDB(outFolder+"/tmp/wdc-url"+id+".db")
                    .fileMmapEnable()
                    .allocateStartSize(1 * 1024 * 1024 * 1024)  // 1GB
                    .allocateIncrement(512 * 1024 * 1024)       // 512MB
                    .make();
            urlCache=db.hashMap("url-cache", Serializer.STRING, Serializer.STRING).createOrOpen();


            Map<String, Integer> propFreq = new HashMap<>();
            Map<String, Integer> classFreq = new HashMap<>();
            Map<String, Integer> hostFreq = new HashMap<>();

            Map<String, Map<String, Integer>> hostPropFreqDetail = new HashMap<>();
            Map<String, Map<String, Integer>> hostClassFreqDetail = new HashMap<>();
            Map<String, Map<String, Integer>> propInHostFreqDetail = new HashMap<>();
            Map<String, Map<String, Integer>> classInHostFreqDetail = new HashMap<>();

            LOG.info("Processing " + inputGZFile);
            LOG.info("\t downloading..." + inputGZFile);
            URL downloadFrom = new URL(inputGZFile);
            File downloadTo = new File(this.outFolder + "/" + new File(downloadFrom.getPath()).getName());
            FileUtils.copyURLToFile(downloadFrom, downloadTo);

            long lines = 0;
            String content;
            LOG.info("\t reading and processing file..." + inputGZFile);
            Scanner inputScanner = setScanner(downloadTo.toString());
            while (inputScanner.hasNextLine() && (content = inputScanner.nextLine()) != null) {
                lines++;

            /*
            Parsing the s, p, o, and source
             */
                try {
                    String subject = null, predicate = null, object = null, source = null;

                    //do we have data literal?
                    int firstQuote = content.indexOf("\"");
                    int lastQuote = content.lastIndexOf("\"");
                    //if yes...
                    if (firstQuote != -1 && lastQuote != -1 && lastQuote > firstQuote) {
                        object = content.substring(firstQuote + 1, lastQuote).trim();

                        String[] s_and_p = content.substring(0, firstQuote).trim().split("\\s+");
                        if (s_and_p.length < 2)
                            continue;
                        subject = trimBrackets(s_and_p[0]);
                        predicate = trimBrackets(s_and_p[1]);

                        source = content.substring(lastQuote + 1);
                        int trim = source.indexOf(" ");
                        source = trimBrackets(source.substring(trim + 1, source.lastIndexOf(" ")));
                        if (source.contains(">")) {
                            source = source.substring(0, source.lastIndexOf(">")).trim();
                        }
                    } else { //if no, all four parts of the quad are URIs
                        String[] parts = content.split("\\s+");
                        if (parts.length < 4)
                            continue;
                        subject = trimBrackets(parts[0]);
                        predicate = trimBrackets(parts[1]);
                        object = trimBrackets(parts[2]);
                        source = trimBrackets(parts[3]).trim();
                        if (source.contains(">")) {
                            source = source.substring(0, source.lastIndexOf(">")).trim();
                        }
                    }

                    subject = subject + "|" + source;

                    if (predicate == null)
                        continue;
                    URI sourceURL = new URI(source);

                    indexURL(sourceURL, urlInfo);

                    incrementStats(sourceURL, new URI(predicate), object,
                            propFreq, classFreq, hostFreq, hostPropFreqDetail,
                            hostClassFreqDetail,
                            propInHostFreqDetail, classInHostFreqDetail);

                    lines++;
                    if (lines % commitBatch==0) {
                        LOG.info(String.format("\t\t processsed %d lines for file %s...", lines, inputGZFile));
                        urlInfo.commit();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.warn(String.format("\t\tThread " + id + " encountered problem for quad (skipped): %s",
                            content, ExceptionUtils.getFullStackTrace(e)));
                }

            }

            db.close();
            FileUtils.deleteQuietly(downloadTo);
            FileUtils.deleteQuietly(new File(outFolder+"/tmp/wdc-url"+id+".db"));
            LOG.info("\t saving data..." + inputGZFile);
            save(inputGZFile,
                    propFreq, classFreq, hostFreq, hostPropFreqDetail,
                    hostClassFreqDetail,
                    propInHostFreqDetail, classInHostFreqDetail);

            LOG.info("\t completed processing file..." + inputGZFile);

            try {
                urlInfo.commit();
                //predicatesCoreClient.commit();
            } catch (Exception e) {
                LOG.warn(String.format("\t\tThread " + id + " failed to make the final commit at completion",
                        lines, ExceptionUtils.getFullStackTrace(e)));
            }
        }

        LOG.info("Thread " + id + " indexing completed");
        return 0;
    }

    private void save(String inputFile,
                      Map<String, Integer> propFreq,
                      Map<String, Integer> classFreq,
                      Map<String, Integer> hostFreq,
                      Map<String, Map<String, Integer>> hostPropFreqDetail,
                      Map<String, Map<String, Integer>> hostClassFreqDetail,
                      Map<String, Map<String, Integer>> propInHostFreqDetail,
                      Map<String, Map<String, Integer>> classInHostFreqDetail) throws IOException {
        String filename = new File(inputFile).getName();
        File subfolder= new File(outFolder + "/"+filename);
        subfolder.mkdirs();
        saveCSV(outFolder + "/"+filename+"/prop_" + filename + ".csv", propFreq);
        saveCSV(outFolder + "/"+filename+"/class_" + filename + ".csv", classFreq);
        saveCSV(outFolder + "/"+filename+"/host_" + filename + ".csv", hostFreq);
        saveCSV2(outFolder + "/"+filename+"/host_prop_" + filename + ".csv", hostPropFreqDetail);
        saveCSV2(outFolder + "/"+filename+"/host_class_" + filename + ".csv", hostClassFreqDetail);
        saveCSV2(outFolder + "/"+filename+"/prop_host_" + filename + ".csv", propInHostFreqDetail);
        saveCSV2(outFolder + "/"+filename+"/class_host_" + filename + ".csv", classInHostFreqDetail);
    }

    private void saveCSV(String outFile, Map<String, Integer> data) throws IOException {
        List<String> keys = new ArrayList<>(data.keySet());
        Collections.sort(keys);
        CSVWriter writer = new CSVWriter(new FileWriter(outFile));
        for (String k : keys) {
            int freq = data.get(k);
            String[] values = new String[2];
            values[0] = k;
            values[1] = String.valueOf(freq);
            writer.writeNext(values);
        }
        writer.close();
    }

    private void saveCSV2(String outFile, Map<String, Map<String, Integer>> data) throws IOException {
        List<String> keys = new ArrayList<>(data.keySet());
        Collections.sort(keys);
        CSVWriter writer = new CSVWriter(new FileWriter(outFile));
        for (String k : keys) {
            Map<String, Integer> innerData = data.get(k);
            List<String> innerKeys = new ArrayList<>(innerData.keySet());
            Collections.sort(innerKeys);

            for (int i = 0; i < innerKeys.size(); i++) {
                String innerK = innerKeys.get(i);
                String innerV = String.valueOf(innerData.get(innerK));
                String[] values = new String[4];
                if (i == 0) {
                    values[0] = k;
                    values[1] = String.valueOf(innerKeys.size());
                    values[2] = innerK;
                    values[3] = innerV;
                } else {
                    values[0] = "";
                    values[1] = "";
                    values[2] = innerK;
                    values[3] = innerV;
                }
                writer.writeNext(values);
            }
        }
        writer.close();
    }

    private void incrementStats(URI source, URI predicate, String object,
                                Map<String, Integer> propFreq,
                                Map<String, Integer> classFreq,
                                Map<String, Integer> hostFreq,
                                Map<String, Map<String, Integer>> hostPropFreqDetail,
                                Map<String, Map<String, Integer>> hostClassFreqDetail,
                                Map<String, Map<String, Integer>> propInHostFreqDetail,
                                Map<String, Map<String, Integer>> classInHostFreqDetail) throws MalformedURLException {
        String host = source.getHost();
        updateCount(predicate.toString(), propFreq);
        if (predicate.toString().equalsIgnoreCase("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
            updateCount(object, classFreq);
            updateCount(host, object, hostClassFreqDetail);
            updateCount(object, host, classInHostFreqDetail);
        }
        updateCount(host, hostFreq);

        updateCount(host, predicate.toString(), hostPropFreqDetail);
        updateCount(predicate.toString(), host, propInHostFreqDetail);
    }

    private void updateCount(String key, Map<String, Integer> map) {
        if (map.containsKey(key))
            map.put(key, map.get(key) + 1);
        else
            map.put(key, 1);
    }

    private void updateCount(String key1, String key2, Map<String, Map<String, Integer>> map) {
        if (map.containsKey(key1)) {
            Map<String, Integer> innerMap = map.get(key1);
            if (innerMap.containsKey(key2))
                innerMap.put(key2, innerMap.get(key2) + 1);
            else
                innerMap.put(key2, 1);
        } else {
            Map<String, Integer> innerMap = new HashMap<>();
            innerMap.put(key2, 1);
            map.put(key1, innerMap);
        }
    }

    /**
     * index the url position in the CC corpus
     *
     * @return
     * @throws IOException
     * @throws SolrServerException
     */
    private boolean indexURL(URI url, SolrClient urlInfo) throws IOException, SolrServerException, URISyntaxException {
        String host = url.getHost();

        //todo: if server disappears, warn and stop
        String offset="-1",length="-1",warc="",digest="";
        String source = urlCache.get(url.toString());
        if (source==null) {
            //?url=sheffield.ac.uk&output=json&showNumPages=true
            URI cc = new URI(ccIndexURL +"?url="+ url.toString() + "&output=json");
            try {
                String response = IOUtils.toString(cc, Charset.forName("utf-8"));
                JsonElement jelement = new JsonParser().parse(response);
                JsonObject jobject = jelement.getAsJsonObject();
                digest=jobject.get("digest").getAsString();
                offset=jobject.get("offset").getAsString();
                length=jobject.get("length").getAsString();
                warc=jobject.get("filename").getAsString();
            }catch (Exception e){
                warc="FAIL";
                //System.out.println(".");
            }

            urlCache.put(url.toString(),warc+"|"+offset+"|"+length+"|"+digest);
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", url.toString());
            doc.addField("host", host);
            doc.addField("CC_offset", Long.valueOf(offset));
            doc.addField("CC_length", Long.valueOf(length));
            doc.addField("CC_WARC", warc);
            doc.addField("CC_DIGEST", digest);

            urlInfo.add(doc);
        }

        return true;
    }

    private String trimBrackets(String line) {
        if (line.startsWith("<"))
            line = line.substring(1);
        if (line.endsWith(">"))
            line = line.substring(0, line.length() - 1);
        return line;
    }

    @Override
    protected Integer compute() {
        if (this.gzFiles.size() > maxTasksPerThread) {
            List<NTripleIndexerWorker> subWorkers =
                    new ArrayList<>(createSubWorkers());
            for (NTripleIndexerWorker subWorker : subWorkers)
                subWorker.fork();
            return mergeResult(subWorkers);
        } else {
            try {
                return computeSingleWorker(this.gzFiles);
            } catch (IOException e) {
                LOG.warn(String.format("\t\tunable to read input gz file: %s, \n %s",
                        this.gzFiles.toString(), ExceptionUtils.getFullStackTrace(e)));
                return 0;
            }
        }
    }


    protected List<NTripleIndexerWorker> createSubWorkers() {
        List<NTripleIndexerWorker> subWorkers =
                new ArrayList<>();

        boolean b = false;
        List<String> splitTask1 = new ArrayList<>();
        List<String> splitTask2 = new ArrayList<>();
        for (String s : gzFiles) {
            if (b)
                splitTask1.add(s);
            else
                splitTask2.add(s);
            b = !b;
        }

        NTripleIndexerWorker subWorker1 = createInstance(splitTask1, this.id + 1);
        NTripleIndexerWorker subWorker2 = createInstance(splitTask2, this.id + 2);

        subWorkers.add(subWorker1);
        subWorkers.add(subWorker2);

        return subWorkers;
    }

    /**
     * NOTE: classes implementing this method must call setHashtagMap and setMaxPerThread after creating your object!!
     *
     * @param splitTasks
     * @param id
     * @return
     */
    protected NTripleIndexerWorker createInstance(List<String> splitTasks, int id) {
        NTripleIndexerWorker indexer = new NTripleIndexerWorker(id,
                urlInfo, outFolder, splitTasks, ccIndexURL);
        return indexer;
    }
    /*{
        return new NTripleIndexerApp(id, this.solrClient, splitTasks, maxTasksPerThread, outFolder);
    }*/

    protected int mergeResult(List<NTripleIndexerWorker> workers) {
        Integer total = 0;
        for (NTripleIndexerWorker worker : workers) {
            total += worker.join();
        }
        return total;
    }
}
