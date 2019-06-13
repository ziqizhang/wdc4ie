package uk.ac.shef.ischool.wdcindex.deprecated.indexer;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;

import java.nio.file.Path;

public class Util {
    public static SolrClient getSolrClient(Path solrHome, String coreName) {
        SolrClient solrClient = new EmbeddedSolrServer(solrHome, coreName);
        return solrClient;
    }
}
