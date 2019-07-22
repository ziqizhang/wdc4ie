package uk.ac.shef.ischool.wdcindex.pcd;

import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import org.semanticweb.yars.nx.parser.ParseException;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * this file reads lines of n-quads and index them accordingly to 'entities' and 'predicates' indexes
 * <p>
 * WARNING: this assumes that same entities do not appear twice in the source data! If that's not the case, data indexed
 * may not be complete
 * <p>
 * WARNING: you need to ensure your data are thread-safe, that is, when different parts of data are processed concurrently
 * by different threads, there will not be identical data instances written by different threads
 * <p>
 * <p>
 * <p>
 * /home/zz/Work/wdc4ie/resources/WDC2017-file.list
 * /home/zz/Work/wdc4ie/resources/WDCTest-file.list.txt
 * /home/zz/Work/wdc4ie/resources/output
 * /home/zz/Work/wdc4ie/resources/solr_wdc
 * http://localhost:8080/CC-MAIN-2017-47-index
 */

public class NTripleIndexerWorker_NoCC implements Runnable {
    private SolrClient urlCore;
    private int commitBatch = 50000;
    private int id;

    private String outFolder;
    private String tmpFolder;
    private Pattern p = Pattern.compile("\\<(.*?)\\>");

    private int parserOption = 0; //0 is regex, 1 is nxParser

    private static final Logger LOG = Logger.getLogger(NTripleIndexerWorker_NoCC.class.getName());

    //private DB db;
    private List<String> gzFiles;

    public NTripleIndexerWorker_NoCC(int id, int parserOption,
                                     String outFolder,
                                     List<String> inputGZFiles,
                                     SolrClient urlCore) {
        this.id = id;
        this.parserOption = parserOption;
        this.urlCore = urlCore;

        this.gzFiles = inputGZFiles;
        this.outFolder = outFolder;
        tmpFolder = outFolder + "/tmp";
    }

    private String[] nxParser(String content) {
        try {
            String subject = null, predicate = null, object = null, source = null;
            Node[] quads = NxParser.parseNodes(content);
            if (quads.length < 4)
                return null;
            subject = quads[0].toString();
            if (!(quads[1] instanceof Resource) || !(quads[3] instanceof Resource))
                return null;
            predicate = ((Resource) quads[1]).toURI().toString();
            source = ((Resource) quads[3]).toURI().toString();

            subject = subject + "|" + source;

            if (predicate == null)
                return null;
            if (quads[2] instanceof Resource)
                object = ((Resource) quads[2]).toURI().toString();
            else
                object = quads[2].toString();

            return new String[]{subject, predicate, object, source};
        } catch (Exception e) {
            LOG.warn(String.format("\t\t\t\tThread " + id + " encountered illegal nquad: %s, due to error: %s",
                    content, ExceptionUtils.getFullStackTrace(e)));
            return null;
        }
    }

    /**
     *
     */
    private String lightClean(String content){

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
        }
        composed+=content.substring(prevE);
        return composed;
    }

    /**
     * @param content
     * @return the 3rd (index=2) element could be null, if it is not a uri
     * @throws Exception
     */
    private String[] regexParser(String content) throws Exception {
        //split by space, which is the separator for nquad format
        if (content.endsWith(".")) {
            content = content.substring(0, content.length() - 1).replaceAll("\\s+", " ").trim();
        }
        String[] parts = content.split("\\s+");

        if (parts.length < 3) {
            LOG.warn(String.format("\t\t\t\tThread " + id + " encountered illegal nquad: %s",
                    content));
            return null;
        }
        //check each part, the first, and second should be either nodes or uris, so they must not have space and complete in itself.
        String subject = parts[0];
        String predicate = parts[1];
        String object=parts[2];

        //is predicate legal?
        if (!predicate.startsWith("<") || !predicate.endsWith(">")) {
            LOG.warn(String.format("\t\t\t\tThread " + id + " encountered illegal nquad: %s",
                    content));
            return null;
        }

        predicate = trimBrackets(predicate);
        subject = trimBrackets(subject);
        if (object.startsWith("<")&&object.endsWith(">"))
            object=trimBrackets(object);
        else
            object=null;

        //check source. If there is one, it must be a complete URI to be legal and should be the last element in array
        String source = parts[parts.length - 1];
        if (parts.length < 4 || !source.startsWith("<") || !source.endsWith(">")) {
            LOG.warn(String.format("\t\t\t\tThread " + id + " encountered illegal nquad: %s",
                    content));
            return null;
        }
        source = trimBrackets(source);

        return new String[]{subject, predicate, object, source};
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

    public void run() {
        int countFiles = 0;
        long countIllegalLines = 0;
        LOG.info("THREAD " + id + " has " + gzFiles.size() + " to process.");

        for (String inputGZFile : gzFiles) {

            countFiles++;
            try {
                /*db = DBMaker.fileDB(outFolder + "/tmp/wdc-url" + id + ".db")
                        .fileMmapEnable()
                        .allocateStartSize(1 * 1024 * 1024 * 1024)  // 1GB
                        .allocateIncrement(512 * 1024 * 1024)       // 512MB
                        .make();
                Map<String, String> urlCache =
                        db.hashMap("url-cache", Serializer.STRING, Serializer.STRING).createOrOpen();
*/

                Map<String, Integer> propFreq = new HashMap<>();
                Map<String, Integer> classFreq = new HashMap<>();
                Map<String, Integer> hostFreq = new HashMap<>();

                Map<String, Map<String, Integer>> hostPropFreqDetail = new HashMap<>();
                Map<String, Map<String, Integer>> hostClassFreqDetail = new HashMap<>();
                Map<String, Map<String, Integer>> propInHostFreqDetail = new HashMap<>();
                Map<String, Map<String, Integer>> classInHostFreqDetail = new HashMap<>();

                LOG.info("THREAD " + id + " Processing " + countFiles + "/" + gzFiles.size() + ", " + inputGZFile);
                LOG.info("\t thread " + id + " downloading..." + inputGZFile);
                URL downloadFrom = new URL(inputGZFile);
                File downloadTo = new File(this.outFolder + "/" + new File(downloadFrom.getPath()).getName());
                FileUtils.copyURLToFile(downloadFrom, downloadTo);

                long lines = 0;
                String content;
                LOG.info("\t thread " + id + " reading and processing file..." + inputGZFile);
                Scanner inputScanner = setScanner(downloadTo.toString());
                while (inputScanner.hasNextLine() && (content = inputScanner.nextLine()) != null) {
                    lines++;
                    /*if (lines==1390000)
                        System.out.println();*/
            /*
            Parsing the s, p, o, and source
            */
            //\[(.*?)\]
                    content=lightClean(content);
                    String[] spos;
                    if (parserOption == 1) {
                        LOG.info("\t thread " + id + " nxparser registered...");
                        spos = nxParser(content);
                    } else {
                        LOG.info("\t thread " + id + " regex parser registered...");
                        spos = regexParser(content);
                    }
                    if (spos == null) {
                        LOG.info(String.format("\t\t thread " + id + " cannot parse line=%d: %s",
                                lines, content));
                        countIllegalLines++;
                        continue;
                    }

                    String subject = spos[0], predicate = spos[1], object = spos[2], source = spos[3];

                    URI sourceURL;

                    try {
                        sourceURL = new URI(source);
                        incrementStats(sourceURL, predicate, object,
                                propFreq, classFreq, hostFreq, hostPropFreqDetail,
                                hostClassFreqDetail,
                                propInHostFreqDetail, classInHostFreqDetail);
                    } catch (Exception e) {
                        LOG.info(String.format("\t\t thread " + id + " cannot parse line %d due to illegal source URI: %s, error=%s",
                                lines, source, ExceptionUtils.getFullStackTrace(e)));
                        countIllegalLines++;
                        continue;
                    }

                    if (urlCore != null)
                        indexURL(sourceURL, urlCore, downloadTo.getName());

                    if (lines % commitBatch == 0) {
                        LOG.info(String.format("\t\t thread " + id + " processsed %d lines for file %s...", lines, inputGZFile));
                        if (urlCore != null)
                            urlCore.commit();
                    }

                }

                //db.commit();
                boolean deleted = false;
                try {
                    inputScanner.close();
                    deleted = downloadTo.delete();
                    //FileUtils.forceDelete(downloadTo);
                } catch (Exception e) {
                    LOG.info("\t thread " + id + " deleting gz file error " + inputGZFile);
                    LOG.info("\t thread " + id + " error:" + ExceptionUtils.getFullStackTrace(e));
                }

                try {
                    LOG.info("\t thread " + id + " saving data..." + inputGZFile);
                    save(inputGZFile,
                            propFreq, classFreq, hostFreq, hostPropFreqDetail,
                            hostClassFreqDetail,
                            propInHostFreqDetail, classInHostFreqDetail
                    );
                    PrintWriter p = new PrintWriter(new FileWriter(tmpFolder + "/" + id + ".job", true));
                    p.println(inputGZFile+","+countIllegalLines+","+lines);
                    p.close();
                    countIllegalLines=0;
                } catch (Exception e) {
                    LOG.info("\t thread " + id + " saving data error " + inputGZFile);
                    LOG.info("\t thread " + id + " error:" + ExceptionUtils.getFullStackTrace(e));
                }

                try {
                    if (urlCore != null)
                        urlCore.commit();
                    Thread.sleep(5000);
                } catch (Exception e) {
                }
                //db.close();

                //FileUtils.deleteQuietly(new File(outFolder + "/tmp/wdc-url" + id + ".db"));

                LOG.info("\t thread " + id + " completed processing file (delted=" + deleted + ")" + countFiles + "/" + gzFiles.size()
                        + ":" + inputGZFile);
            } catch (Exception e) {
                e.printStackTrace();
                LOG.warn(String.format("\t\tThread " + id + " encountered problem for GZ file %s, %s",
                        inputGZFile, ExceptionUtils.getFullStackTrace(e)));
            }
        }

        LOG.info("Thread " + id + " indexing completed, newly_processed=" + countFiles + ", total=" + gzFiles.size());
    }

    private void save(String inputFile,
                      Map<String, Integer> propFreq,
                      Map<String, Integer> classFreq,
                      Map<String, Integer> hostFreq,
                      Map<String, Map<String, Integer>> hostPropFreqDetail,
                      Map<String, Map<String, Integer>> hostClassFreqDetail,
                      Map<String, Map<String, Integer>> propInHostFreqDetail,
                      Map<String, Map<String, Integer>> classInHostFreqDetail/*,
                      Map<String, String> urlCache*/) throws IOException {
        String filename = new File(inputFile).getName().replaceAll("\\.", "_");
        new File(outFolder + "/" + filename).mkdirs();
        LOG.info("\t thread " + id + " saving prop...");
        saveCSV(outFolder + "/" + filename + "/prop_" + filename + ".csv", propFreq);
        LOG.info("\t thread " + id + " saving class...");
        saveCSV(outFolder + "/" + filename + "/class_" + filename + ".csv", classFreq);
        LOG.info("\t thread " + id + " saving host...");
        saveCSV(outFolder + "/" + filename + "/host_" + filename + ".csv", hostFreq);
        LOG.info("\t thread " + id + " saving host_prop...");
        saveCSV2(outFolder + "/" + filename + "/host_prop_" + filename + ".csv", hostPropFreqDetail);
        LOG.info("\t thread " + id + " saving host_class...");
        saveCSV2(outFolder + "/" + filename + "/host_class_" + filename + ".csv", hostClassFreqDetail);
        LOG.info("\t thread " + id + " saving prop_host...");
        saveCSV2(outFolder + "/" + filename + "/prop_host_" + filename + ".csv", propInHostFreqDetail);
        LOG.info("\t thread " + id + " saving class_host...");
        saveCSV2(outFolder + "/" + filename + "/class_host_" + filename + ".csv", classInHostFreqDetail);
        LOG.info("\t thread " + id + " saving url cache...");
        //todo: saving urlcache
        /*CSVWriter writer =
                new CSVWriter(new FileWriter(outFolder + "/"+filename+"/url_source" + filename + ".csv"));
        for (Map.Entry<String, String> entry : urlCache.entrySet()) {
            String key=entry.getKey();
            String[] values=entry.getValue().split("\t");
            String[] nvalues = new String[values.length+1];
            nvalues[0] = key;
            for (int i=0;i<values.length;i++)
                nvalues[i+1] = values[i];
            writer.writeNext(nvalues);
        }
        writer.close();*/
    }

    private void saveCSV(String outFile, Map<String, Integer> data) throws IOException {
        List<String> keys = new ArrayList<>(data.keySet());
        /*if (keys == null)
            System.out.println("KEYS ARE NULL");
        if (keys.contains(null))
            System.out.println("KEYS HAVE NULL");*/
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

    private void incrementStats(URI source, String predicate, String object,
                                Map<String, Integer> propFreq,
                                Map<String, Integer> classFreq,
                                Map<String, Integer> hostFreq,
                                Map<String, Map<String, Integer>> hostPropFreqDetail,
                                Map<String, Map<String, Integer>> hostClassFreqDetail,
                                Map<String, Map<String, Integer>> propInHostFreqDetail,
                                Map<String, Map<String, Integer>> classInHostFreqDetail) throws MalformedURLException {
        String host = source.getHost();
        updateCount(predicate, propFreq);
        if (predicate.equalsIgnoreCase("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && object!=null) {
            updateCount(object, classFreq);
            updateCount(host, object, hostClassFreqDetail);
            updateCount(object, host, classInHostFreqDetail);
        }
        updateCount(host, hostFreq);

        updateCount(host, predicate, hostPropFreqDetail);
        updateCount(predicate, host, propInHostFreqDetail);
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
    /*private boolean indexURL(URI url, Map<String, String> urlCache) throws URISyntaxException {
        String host = url.getHost();

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

            urlCache.put(url.toString(),warc+"\t"+host+"\t"+offset+"\t"+length+"\t"+digest);
        }

        return true;
    }*/
    private boolean indexURL(URI url, SolrClient urlInfo, String gzFilename) throws IOException, SolrServerException, URISyntaxException {
        String host = url.getHost();

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", url.toString());
        doc.addField("host", host);
        doc.addField("wdc_gz", gzFilename);

        urlInfo.add(doc);

        return true;
    }

    private String trimBrackets(String line) {
        if (line.startsWith("<"))
            line = line.substring(1);
        if (line.endsWith(">"))
            line = line.substring(0, line.length() - 1);
        return line;
    }

}
