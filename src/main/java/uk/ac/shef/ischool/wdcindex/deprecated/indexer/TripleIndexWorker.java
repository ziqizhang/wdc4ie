package uk.ac.shef.ischool.wdcindex.deprecated.indexer;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.semanticweb.yars.nx.Literal;
import org.semanticweb.yars.nx.Node;
import org.semanticweb.yars.nx.Resource;
import org.semanticweb.yars.nx.parser.NxParser;
import uk.ac.shef.ischool.wdcindex.Worker;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class TripleIndexWorker extends Worker {
    private static final Logger LOG = Logger.getLogger(TripleIndexWorker.class.getName());
    private static final int commitBatch = 1000;
    private Object ccIndex;
    private static Set<String> keepTLDs = new HashSet<>(Arrays.asList("com", "org","net",
            "edu","gov","mil","uk","au","ag","bs","bb","bz","ca","dm","gd","gy","ie","jm",
            "nz","kn","lc","vc","tt","us","vi","eu"));

    public TripleIndexWorker(int id, SolrClient solrClient,
                             Object ccIndex, List<String> jobs) {
        super(id, solrClient, jobs);
        this.ccIndex = ccIndex;
    }

    @Override
    protected int computeSingleWorker(List<String> jobs) {
        int total = 0;

        for (String file : jobs) {
            LOG.info(String.format("Processing %s", file));
            String filename = new File(file).getName();
            if (file.endsWith(".gz")) {
                try{
                    downloadFile(file);
                }catch (Exception e){
                    LOG.warn(String.format("\tfile %s cannot be downloaded, skipped...",
                            file, ExceptionUtils.getFullStackTrace(e)));
                    continue;
                }


                try {
                    Scanner inputScanner = setScanner(filename);
                    String content;

                    int lines = 0;
                    while (inputScanner.hasNextLine() && (content = inputScanner.nextLine()) != null) {
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
                    deleteFile(filename);
                    LOG.info(String.format("\tfile completed, total lines=%d", lines));
                }
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

    private void downloadFile(String urlStr) throws IOException {
        URL url = new URL(urlStr);
        ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
        FileOutputStream fileOutputStream = new FileOutputStream(url.getFile());
        FileChannel fileChannel = fileOutputStream.getChannel();
        fileOutputStream.getChannel()
                .transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
    }

    private void deleteFile(String file){
        new File(file).delete();
    }

    private Scanner setScanner(String file) throws IOException {
        InputStream fileStream = new FileInputStream(file);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, Charset.forName("utf8"));
        Scanner inputScanner = new Scanner(decoder);
        inputScanner.useDelimiter(" .");
        LOG.info("Thread "+id+" Obtained scanner object in put file");
        return inputScanner;
    }

    private void indexNQuad(String lineContent, String nquadFile, int line) {
        String id = nquadFile + "_" + line;
        try {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField(TripleIndexField.F_ID.getFieldname(), id);

            Node[] quads = NxParser.parseNodes(lineContent);
            Node object = quads[2];

            boolean ignore=false;
            try {
                List<String> objectTypes = parseObjTypes(object);
                doc.addField(TripleIndexField.F_OBJECT_TYPE.getFieldname(), objectTypes);
            }catch (InvalidObjectException e){
                ignore=true;
            }

            String tld = "";
            String host = "";
            if (quads[3] instanceof Resource) {
                host = ((Resource) quads[3]).toURI().getHost();
                String[] domainNameParts = host.split("\\.");
                tld = domainNameParts[domainNameParts.length - 1];
                if (!keepTLDs.contains(tld))
                    ignore=true;
            }

            if (!ignore) {
            /*String subject = quads[0].getLabel();
            String subjectNS = parseNamespace(subject);
            String subjectText = subject.replaceAll("[^a-zA-Z0-9]", " ").trim();
            doc.addField(TripleIndexField.F_SUBJECT.getFieldname(), subject);
            doc.addField(TripleIndexField.F_SUBJECT_NAMESPACE.getFieldname(), subjectNS);
            doc.addField(TripleIndexField.F_SUBJECT_TEXT.getFieldname(), subjectText);*/
                doc.addField(TripleIndexField.F_TLD.getFieldname(), tld);
                doc.addField(TripleIndexField.F_HOST.getFieldname(), host);
                doc.addField(TripleIndexField.F_PROVENANCE_URL.getFieldname(), quads[3].getLabel());

                String predicate = quads[1].getLabel();
                String predicatetNS = parseNamespace(predicate);
                String predicateText = predicate.replaceAll("[^a-zA-Z0-9]", " ").trim();
                doc.addField(TripleIndexField.F_PREDICATE.getFieldname(), predicate);
                doc.addField(TripleIndexField.F_PREDICATE_NAMESPACE.getFieldname(), predicatetNS);
                //doc.addField(TripleIndexField.F_PREDICATE_TEXT.getFieldname(), predicateText);


                String objID = object.getLabel();
                if (objID.length() > 200)
                    objID = objID.substring(0, 200);
                String objectNS = "";
                if (!(object instanceof Literal))
                    objectNS = parseNamespace(object.getLabel());
                String objectText = objID.replaceAll("[^a-zA-Z0-9]", " ").trim();


                doc.addField(TripleIndexField.F_OBJECT.getFieldname(), objID);
                doc.addField(TripleIndexField.F_OBJECT_NAMESPACE.getFieldname(), objectNS);
                //doc.addField(TripleIndexField.F_OBJECT_TEXT.getFieldname(), objectText);


                String wdcFormat = nquadFile;
                if (wdcFormat.contains(".nq"))
                    wdcFormat = wdcFormat.substring(0, wdcFormat.indexOf(".nq"));
                doc.addField(TripleIndexField.F_PROVENANCE_WDC_FORMAT.getFieldname(), wdcFormat);
                doc.addField(TripleIndexField.F_PROVENANCE_WDC_NQUAD_FILE.getFieldname(), nquadFile);
                doc.addField(TripleIndexField.F_PROVENANCE_WDC_NQUAD_LINE.getFieldname(), line);

                //todo: query ccIndex to find provenance in the CC corpus
                String provCCWarc = "";
                String provCCOffset = ",";
                doc.addField(TripleIndexField.F_PROVENANCE_CC_WARC.getFieldname(), provCCWarc);
                doc.addField(TripleIndexField.F_PROVENANCE_CC_WARC_OFFSET.getFieldname(), provCCOffset);

                solrClient.add(doc);
            }

        } catch (Exception e) {
            LOG.error(String.format("\t\tline %d in file %s encountered an error, skipped: %s. Error: \n\t\t %s \n",
                    line, nquadFile, lineContent, ExceptionUtils.getFullStackTrace(e)));
        }

    }

    private List<String> parseObjTypes(Node object) throws InvalidObjectException {
        List<String> types = new ArrayList<>();
        if (object instanceof Literal) {
            Literal l = (Literal) object;
            if (l.getLanguageTag() != null) {
                if (!l.getLanguageTag().equalsIgnoreCase("en"))
                    throw new InvalidObjectException("non english data");
                types.add(l.getLanguageTag());
            }
            if (l.getDatatype() != null)
                types.add(l.getDatatype().getLabel());
            types.add("literal");
        } else {
            types.add("uri");
        }
        return types;
    }

    private String parseNamespace(String value) {
        value = value.substring(1, value.length() - 1).trim();

        int end1 = value.lastIndexOf("#");
        int end2 = value.lastIndexOf("/");
        int end = end1 > end2 ? end1 : end2;
        return value.substring(0, end + 1);
    }

    private URI parseURI(String value) {
        if (value.startsWith("<"))
            value = value.substring(1).trim();
        if (value.endsWith(">"))
            value = value.substring(0, value.length() - 1).trim();

        try {
            return new URI(value);
        } catch (URISyntaxException e) {
            return null;
        }
    }

    @Override
    protected TripleIndexWorker createInstance(List<String> jobs, int id) {
        return new TripleIndexWorker(id, this.solrClient,
                this.ccIndex, jobs);
    }
}
