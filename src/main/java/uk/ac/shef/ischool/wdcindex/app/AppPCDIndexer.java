package uk.ac.shef.ischool.wdcindex.app;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import uk.ac.shef.ischool.wdcindex.pcd.PCDExporterMaster;
import uk.ac.shef.ischool.wdcindex.indexer.Util;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;

public class AppPCDIndexer {
    public static void main(String[] args) throws IOException, SolrServerException {
        SolrClient solrClient =
                Util.getSolrClient(Paths.get(args[0]), "pcd");

        PCDExporterMaster processor = new PCDExporterMaster(solrClient, args[1],args[2]);
        processor.process();
        System.out.println(String.format("All jobs completed, optimising the index ... %s", new Date().toString()));
        solrClient.optimize();
        solrClient.close();

    }
}
