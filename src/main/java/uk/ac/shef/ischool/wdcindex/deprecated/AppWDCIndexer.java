package uk.ac.shef.ischool.wdcindex.deprecated;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import uk.ac.shef.ischool.wdcindex.deprecated.indexer.TripleIndexMaster;
import uk.ac.shef.ischool.wdcindex.deprecated.indexer.Util;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;

public class AppWDCIndexer {

    public static void main(String[] args) throws IOException, SolrServerException {
        SolrClient solrClient =
                Util.getSolrClient(Paths.get(args[0]), "triples");

        TripleIndexMaster processor = new TripleIndexMaster(solrClient, args[1]);
        processor.process();
        System.out.println(String.format("All jobs completed, optimising the index ... %s", new Date().toString()));
        solrClient.optimize();
        solrClient.close();

    }
}
