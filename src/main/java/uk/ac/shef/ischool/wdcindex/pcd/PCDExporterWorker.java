package uk.ac.shef.ischool.wdcindex.pcd;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import uk.ac.shef.ischool.wdcindex.Worker;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * PCD stands for property, class, domain
 *
 * process each quad.gz file, each property and each class will be written to separate files, and each file will
 * have a max of N lines.
 *
 * at the same time, property-domain and class-domain pairs are recorded in a solr index
 */
public class PCDExporterWorker extends Worker {
    private static final Logger LOG = Logger.getLogger(PCDExporterWorker.class.getName());
    private static final String ISA="http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    private static final int linesPerFile=6000000;
    private static final int commitBatch = 5000;
    private Object ccIndex;
    private String outFolder;

    private Map<String, Integer> predicateFreq=new HashMap<>();
    private Map<String, List<String>> predicateCache=new HashMap<>();
    private Map<String, Integer> classFreq=new HashMap<>();
    private Map<String, List<String>> classCache=new HashMap<>();

    private Map<String, String> filenameLookup=new HashMap<>();

    private static Set<String> englishTLDs = new HashSet<>(Arrays.asList("com", "org","net",
            "edu","gov","mil","uk","au","ag","bs","bb","bz","ca","dm","gd","gy","ie","jm",
            "nz","kn","lc","vc","tt","us","vi","eu"));

    public PCDExporterWorker(int id, SolrClient solrClient,
                             Object ccIndex, List<String> jobs, String outFolder) {
        super(id, solrClient, jobs);
        this.ccIndex = ccIndex;
        this.outFolder=outFolder+"/"+id;
        new File(this.outFolder).mkdirs();
    }

    @Override
    protected int computeSingleWorker(List<String> jobs) {
        int total = 0;

        for (String file : jobs) {
            LOG.info(String.format("Processing %s", file));
            String filename = new File(file).getName();
            if (file.endsWith(".gz")) {
                //todo: download the file locally
                try {
                    BufferedReader in = new BufferedReader(new InputStreamReader(
                            new GZIPInputStream(new FileInputStream(file)), Charset.forName("utf8")));

                    String content;

                    int lines = 0;
                    while ((content = in.readLine()) != null) {
                        //each line is an nquad, process and index the nquad
                        indexNQuad(content, filename, lines);

                        lines++;
                        if (lines % commitBatch == 0) {
                            try {
                                solrClient.commit();
                            } catch (Exception e) {
                                LOG.warn(String.format("\tbatch to commit failed (final instance=%d, batch size=%d) with an exception: \n\t %s \n\t trying for the next file...",
                                        total, commitBatch, ExceptionUtils.getFullStackTrace(e)));
                            }
                        }
                        if (lines%200000==0)
                            LOG.info(lines);
                    }

                    flushCache(predicateCache, predicateFreq,"P");
                    flushCache(classCache, classFreq,"C");
                    LOG.info(String.format("\tfile completed, total lines=%d", lines));
                }
                //todo: delete the file from local store
                catch (IOException ioe) {
                    LOG.warn(String.format("\tfile %s caused an exception: \n\t %s \n\t trying for the next file...",
                            file, ExceptionUtils.getFullStackTrace(ioe)));
                }
                try {
                    solrClient.commit();
                } catch (Exception e) {
                    LOG.warn(String.format("\tbatch to commit failed (final instance=%d, batch size=%d) with an exception: \n\t %s \n\t trying for the next file...",
                            total, commitBatch, ExceptionUtils.getFullStackTrace(e)));
                }
                total++;
            } else {
                LOG.error(String.format("\tfile format not supported, skipped: %s", file));
            }
        }


        return total;
    }


    private void indexNQuad(String lineContent, String nquadFile, int line) {
        try {
            SolrInputDocument doc = new SolrInputDocument();

            Node[] quads = NxParser.parseNodes(lineContent);
            Node object = quads[2];

            boolean ignore=false;
            String tld = "";
            String host = "";
            if (quads[3] instanceof Resource) {
                host = ((Resource) quads[3]).toURI().getHost();
                String[] domainNameParts = host.split("\\.");
                tld = domainNameParts[domainNameParts.length - 1];
                /*if (!englishTLDs.contains(tld))
                    ignore=true;*/
            }

            if (!ignore) {
            /*String subject = quads[0].getLabel();
            String subjectNS = parseNamespace(subject);
            String subjectText = subject.replaceAll("[^a-zA-Z0-9]", " ").trim();
            doc.addField(TripleIndexField.F_SUBJECT.getFieldname(), subject);
            doc.addField(TripleIndexField.F_SUBJECT_NAMESPACE.getFieldname(), subjectNS);
            doc.addField(TripleIndexField.F_SUBJECT_TEXT.getFieldname(), subjectText);*/
                String predicate = quads[1].getLabel();
                String id = host+"|"+predicate;
                if (predicate.equalsIgnoreCase(ISA)) {
                    id = predicate + "|" + quads[2].getLabel();
                    doc.addField(PCDIndexField.F_CLASS.getFieldname(), quads[2].getLabel());
                }

                doc.addField(PCDIndexField.F_ID.getFieldname(), id);
                doc.addField(PCDIndexField.F_HOST.getFieldname(), host);
                doc.addField(PCDIndexField.F_PREDICATE.getFieldname(), predicate);
                solrClient.add(doc);

                String sourceURL=quads[3].getLabel();

                //todo: query ccIndex to find provenance in the CC corpus
                String provCCWarc = "cc-warc";
                String provCCOffset = "0\t0";

                StringBuilder s = new StringBuilder(nquadFile);
                s.append("\t").append(line).append("\t").append(provCCWarc).append("\t")
                        .append(provCCOffset).append("\t").append(sourceURL).append("\t")
                        .append(host).append("\t").append(tld);
                writeData(predicate, s.toString(), predicateCache, predicateFreq,
                        "P");

                if (predicate.equalsIgnoreCase(ISA))
                    writeData(quads[2].getLabel(), s.toString(),classCache,classFreq,
                            "C");

            }

        } catch (Exception e) {
            LOG.error(String.format("\t\tline %d in file %s encountered an error, skipped: %s. Error: \n\t\t %s \n",
                    line, nquadFile, lineContent, ExceptionUtils.getFullStackTrace(e)));
        }

    }

    private void flushCache(Map<String, List<String>> cache, Map<String, Integer> freqs,
                            String propertyOrClass) throws IOException {
        for(Map.Entry<String, List<String>> e : cache.entrySet()){
            if (e.getValue().size()>0){
                Integer freq = freqs.get(e.getKey());
                freq=freq==null?0:freq;
                Writer w = obtainWriter(e.getKey(), freq, propertyOrClass);
                for (String l: e.getValue())
                    w.write(l+"\n");
                w.close();
            }
        }
    }

    /**
     *
     * @param uri
     * @param data
     */
    private void writeData(String uri, String data, Map<String, List<String>> cache, Map<String, Integer> freqs,
                           String propertyOrClass) throws IOException {

        String filename = encodeFilename(uri);
        filenameLookup.put(propertyOrClass+"_"+uri,
                propertyOrClass+"_"+filename);

        List<String> cachedData;
        cachedData = cache.get(uri);
        Integer freq = freqs.get(uri);
        if (freq==null)
            freq=0;
        freq++;
        freqs.put(uri, freq);

        if (cachedData==null)
            cachedData=new ArrayList<>();
        cachedData.add(data);

        if (cachedData.size()>100) {
            //write data,
            Writer w = obtainWriter(uri, freq,propertyOrClass);
            for (String l: cachedData)
                w.write(l+"\n");
            w.close();
            cachedData.clear();
        }

        cache.put(uri, cachedData);

    }

    private Writer obtainWriter(String uri, Integer freq,
                                String propertyOrClass) throws IOException {
        String filename=filenameLookup.get(propertyOrClass+"_"+uri);
        if (filename==null)
            System.out.println();
        filename=outFolder+"/"+filename;
        int suffix = freq/linesPerFile;
        filename+="-"+suffix;
        FileWriter fw = new FileWriter(filename, true);
        BufferedWriter bw = new BufferedWriter(fw);
        return bw;
    }

    private String encodeFilename(String uri) {
        return uri.replaceAll("[\\*/\\\\!\\|:?<>]", "_")
                .replaceAll("(%22)", "_");
    }


    @Override
    protected PCDExporterWorker createInstance(List<String> jobs, int id) {
        return new PCDExporterWorker(id, this.solrClient,
                this.ccIndex, jobs, this.outFolder);
    }

}
