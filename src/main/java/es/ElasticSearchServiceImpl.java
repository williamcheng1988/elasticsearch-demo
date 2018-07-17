package es;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class ElasticSearchServiceImpl {

    ElasticSearchManger elasticSearchManger = new ElasticSearchManger();

    // 单个数据添加
    public Boolean add(Map<String, Object> doc) throws Exception {
        if (doc != null) {
            try {
                XContentBuilder json = XContentFactory.jsonBuilder().startObject();
                Iterator<String> iterator = doc.keySet().iterator();
                while (iterator.hasNext()) {
                    String key = iterator.next();
                    Object object = doc.get(key);
                    if (object instanceof Integer)
                        json.field(key, Integer.valueOf(object.toString()));
                    else if (object instanceof Long)
                        json.field(key, Long.valueOf(object.toString()));
                    else if (object instanceof String)
                        json.field(key, object.toString());
                    else
                        json.field(key, object);
                }
                json.endObject();
                elasticSearchManger.client.prepareIndex("bbg_goods", "jdbc").setSource(json).get();
                return true;
            } catch (Exception e) {
                throw e;
            }
        }
        return false;
    }

    public Boolean adds(List<Map<String, Object>> docs) {
        return true;
    }

    public Boolean searchBulkIn(List<EsData> datas) throws Exception {
        return searchBulkIn("william-orders", "goods", datas);
    }

    public Boolean searchBulkIn(String index, String type, List<EsData> datas) throws Exception {
        try {
            BulkRequestBuilder bulkRequest = elasticSearchManger.client.prepareBulk();
            for (EsData data : datas) {
                bulkRequest.add(elasticSearchManger.client.prepareIndex(index, type).setId(data.getId()).setSource(data.getData()));
            }
            bulkRequest.execute().actionGet();
            return true;
        } catch (Exception e) {
            throw e;
        }
    }

    public Boolean delete() {
        return true;
    }

    public SearchResponse query() {
        return null;
    }
}
