package solr;

import java.util.List;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexServerImpl {

    SolrSearchManger manger = new SolrSearchManger();

    public Boolean addDocuments( List<SolrInputDocument> docs) {
        SolrServer client = manger.solrServer;
        if (client == null) {
            return false;
        }
        try {
            UpdateResponse resp = client.add(docs);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
    public Boolean commit() {
        SolrServer client = manger.solrServer;
        try {
            client.commit();
        } catch (Exception e) {
            return false;
        }
        return true;
    }
}
