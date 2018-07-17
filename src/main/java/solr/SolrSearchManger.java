package solr;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrSearchManger {
	
	private static Logger logger = LoggerFactory.getLogger(SolrSearchManger.class);
    CloudSolrServer solrServer;

    public SolrSearchManger() {
        init();
    }

    public void init() {
        solrServer = new CloudSolrServer("192.168.0.105:2181");
        // set default collection
        solrServer.setDefaultCollection("goods");
        //
        solrServer.connect();
        
    }

}
