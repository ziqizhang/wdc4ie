package uk.ac.shef.ischool.wdcindex.table;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class WDCWebtableOverlapChecker {
    private static final String WEBTABLE_CORPUS_URL =
            "http://data.dws.informatik.uni-mannheim.de/webtables/2015-07/completeCorpus/compressed/";

    private static final int commitBatch = 10000;

    private static final Logger LOG =
            Logger.getLogger(WDCWebtableOverlapChecker.class.getName());

    private DB db = DBMaker.fileDB("WDCWebtableOverlapChecker.db").make();
    private Map<String, Integer> relationTableSourceURL_tripleFreq
            = db.hashMap("REL-url_3freq", Serializer.STRING, Serializer.INTEGER).createOrOpen();
    private Map<String, Integer> entityTableSourceURL_tripleFreq
            = db.hashMap("ENT-url_3freq", Serializer.STRING, Serializer.INTEGER).createOrOpen();

    private Map<String, Integer> relationTableSourceDomain_tripleFreq
            = db.hashMap("REL-domain_3freq", Serializer.STRING, Serializer.INTEGER).createOrOpen();
    private Map<String, Integer> entityTableSourceDomain_tripleFreq
            = db.hashMap("ENT-domain_3freq", Serializer.STRING, Serializer.INTEGER).createOrOpen();
    private Gson gson = new Gson();

    public void process(String webtableList, String wdcList, String tmpFolder, int start, int end) throws IOException {
        processWebTableData(webtableList, tmpFolder);
        db.close();
        System.exit(0);

        processWDCTriples(wdcList, tmpFolder, start, end);

        long sum=0, totalNonZero=0, total=0;
        System.out.println(String.format("relation table stats url... %d", relationTableSourceURL_tripleFreq.size()));
        for (Map.Entry<String, Integer> e : relationTableSourceURL_tripleFreq.entrySet()){
            total++;
            if (e.getValue()>0) {
                totalNonZero++;
                sum += e.getValue();
            }
            if (total%100000==0)
                System.out.println("\t\t"+total);
        }
        System.out.println(String.format("\nTotal relation table source urls=%d, overlap in wdc=%d," +
                " avg triples=%d", relationTableSourceURL_tripleFreq.size(),
                totalNonZero, sum/totalNonZero));

        sum=0;
        totalNonZero=0;
        total=0;
        System.out.println(String.format("relation table stats domain... %d", relationTableSourceDomain_tripleFreq.size()));
        for (Map.Entry<String, Integer> e : relationTableSourceDomain_tripleFreq.entrySet()){
            total++;
            if (e.getValue()>0) {
                totalNonZero++;
                sum += e.getValue();
            }
            if (total%100000==0)
                System.out.println("\t\t"+total);
        }
        System.out.println(String.format("\nTotal relation table source domain=%d, overlap in wdc=%d," +
                        " avg triples=%d", relationTableSourceDomain_tripleFreq.size(),
                totalNonZero, sum/totalNonZero));

        sum=0;totalNonZero=0;total=0;
        System.out.println(String.format("entity table stats url... %d", entityTableSourceURL_tripleFreq.size()));
        for (Map.Entry<String, Integer> e : entityTableSourceURL_tripleFreq.entrySet()){
            total++;
            if (e.getValue()>0) {
                totalNonZero++;
                sum += e.getValue();
            }
            if (total%100000==0)
                System.out.println("\t\t"+total);
        }
        System.out.println(String.format("\nTotal entity table source urls=%d, overlap in wdc=%d," +
                        " avg triples=%d", entityTableSourceURL_tripleFreq.size(),
                totalNonZero, sum/totalNonZero));

        sum=0;
        totalNonZero=0;total=0;
        System.out.println(String.format("entity table stats domain... %d", entityTableSourceDomain_tripleFreq.size()));
        for (Map.Entry<String, Integer> e : entityTableSourceDomain_tripleFreq.entrySet()){
            total++;
            if (e.getValue()>0) {
                totalNonZero++;
                sum += e.getValue();
            }
            if (total%100000==0)
                System.out.println("\t\t"+total);
        }
        System.out.println(String.format("\nTotal entity table source domain=%d, overlap in wdc=%d," +
                        " avg triples=%d", entityTableSourceDomain_tripleFreq.size(),
                totalNonZero, sum/totalNonZero));
    }

    private void processWebTableData(String fileList, String tmpFolder) throws IOException {
        //list of files to download
        List<String> jobs = FileUtils.readLines(new File(fileList),
                Charset.forName("utf8"));

        for (String job : jobs) {
            int total = 0;
            LOG.info(String.format("processing file %s", job));
            String targetLocalFile = tmpFolder + "/" + job;
            String download = WEBTABLE_CORPUS_URL + job;
            FileUtils.copyURLToFile(new URL(download),
                    new File(targetLocalFile));

            File targetUnzipFolder = new File(tmpFolder + "/webtables");
            decompress(targetLocalFile, targetUnzipFolder);

            Collection<File> files = FileUtils.listFiles(targetUnzipFolder, new String[]{"gz"}, true);

            for (File f : files) {
                total++;
                if (!f.getName().endsWith(".gz"))
                    continue;
                //process the file
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        new GZIPInputStream(new FileInputStream(f)), Charset.forName("utf8")));

                String content;

                while ((content = in.readLine()) != null) {
                    //parse json string
                    Map<String, Object> values = gson.fromJson(content,
                            new TypeToken<HashMap<String, Object>>() {
                            }.getType());
                    String tableType = values.get("tableType").toString();
                    String url = values.get("url").toString();
                    URL urlObj = new URL(url);
                    String domain = urlObj.getHost();

                    if (tableType.equalsIgnoreCase("entity")) {
                        relationTableSourceURL_tripleFreq.put(url, 0);
                        relationTableSourceDomain_tripleFreq.put(domain, 0);
                    } else if (tableType.equalsIgnoreCase("relation")) {
                        entityTableSourceURL_tripleFreq.put(url, 0);
                        entityTableSourceDomain_tripleFreq.put(domain, 0);
                    }
                }

                try {
                    db.commit();
                } catch (Exception e) {
                    LOG.warn(String.format("\tbatch to commit failed (final instance=%d, batch size=%d) with an exception: \n\t %s \n\t trying for the next file...",
                            total, commitBatch, ExceptionUtils.getFullStackTrace(e)));
                }
                LOG.info(String.format("\t\tsubfile completed, %d/%d, %s", total,files.size(), f.toString()));
            }
            //delete the file
            new File(targetLocalFile).delete();
            FileUtils.deleteDirectory(targetUnzipFolder);
        }
    }

    private void processWDCTriples(String fileList, String tmpFolder, int start, int end) throws IOException {
        //list of files to download
        List<String> jobs = FileUtils.readLines(new File(fileList),
                Charset.forName("utf8"));
        if (start<0)
            start=0;
        if (end<0)
            end=Integer.MAX_VALUE;

        LOG.info(String.format("start %d and end %d", start, end));
        int total = 0;
        for (String job : jobs) {
            total++;
            if (total<start)
                continue;
            if (total>end)
                break;

            LOG.info(String.format("processing file %s", job));
            String targetLocalFile = tmpFolder + "/" + new File(job).getName();
            FileUtils.copyURLToFile(new URL(job),
                    new File(targetLocalFile));

            File f = new File(targetLocalFile);
            //process the file
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(new FileInputStream(f)), Charset.forName("utf8")));

            String content;

            int count=0;
            while ((content = in.readLine()) != null) {
                //parse json string
                count++;
                try {
                    Node[] quads = NxParser.parseNodes(content);
                    if (quads.length<4)
                        continue;
                    if (quads[3] instanceof Resource) {
                        URI uri = ((Resource) quads[3]).toURI();
                        String host = uri.getHost();
                        String uriStr = uri.toString();
                        if (relationTableSourceURL_tripleFreq.containsKey(uriStr)){
                            Integer freq=relationTableSourceURL_tripleFreq.get(uriStr);
                            if (freq==null)
                                continue;
                            freq++;
                            relationTableSourceURL_tripleFreq.put(uriStr, freq);
                        }
                        if (relationTableSourceDomain_tripleFreq.containsKey(host)){
                            Integer freq=relationTableSourceDomain_tripleFreq.get(host);
                            if (freq==null)
                                continue;
                            freq++;
                            relationTableSourceDomain_tripleFreq.put(host, freq);
                        }
                        if (entityTableSourceURL_tripleFreq.containsKey(uriStr)){
                            Integer freq=entityTableSourceURL_tripleFreq.get(uriStr);
                            if (freq==null)
                                continue;
                            freq++;
                            entityTableSourceURL_tripleFreq.put(uriStr, freq);
                        }
                        if (entityTableSourceDomain_tripleFreq.containsKey(host)){
                            Integer freq=entityTableSourceDomain_tripleFreq.get(host);
                            if (freq==null)
                                continue;
                            freq++;
                            entityTableSourceDomain_tripleFreq.put(host, freq);
                        }
                    }
                }catch (Exception e){
                    LOG.error(String.format("\tprocessing line %d failed due to exception \n\t%s: \n",
                            count, ExceptionUtils.getFullStackTrace(e)));
                }

                if (count%200000==0)
                    System.out.println("\t"+count);
            }

            try {
                db.commit();
            } catch (Exception e) {
                LOG.warn(String.format("\tbatch to commit failed (final instance=%d, batch size=%d) with an exception: \n\t %s \n\t trying for the next file...",
                        total, commitBatch, ExceptionUtils.getFullStackTrace(e)));
            }
            LOG.info(String.format("\t\tsubfile completed, %s", f.toString()));

            //delete the file
            new File(targetLocalFile).delete();
            //System.exit(0);
        }
    }

    private static void decompress(String in, File out) throws IOException {

        out.mkdirs();

        try (TarArchiveInputStream fin =
                     new TarArchiveInputStream(new GZIPInputStream(new FileInputStream(in)))) {
            TarArchiveEntry entry;
            while ((entry = fin.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }
                File curfile = new File(out, entry.getName());
                File parent = curfile.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }
                IOUtils.copy(fin, new FileOutputStream(curfile));
            }
        }
    }
}
