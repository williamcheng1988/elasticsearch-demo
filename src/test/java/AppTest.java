import es.ElasticSearchServiceImpl;
import es.EsData;
import org.json.JSONObject;

import java.util.*;

/**
 * User: William Cheng
 * Create Time: 2018/7/14 20:35
 * Description:
 */
public class AppTest {

    public static void main(String[] args) throws Exception {
        final ElasticSearchServiceImpl elasticSearchServiceImpl = new ElasticSearchServiceImpl();

        List<EsData> datas = new ArrayList<EsData>();
        EsData esData = null;
        Map<String, Object> map = null;
        for (int i = 1; i < 10; i++) {
            esData = new EsData();
            map = new HashMap<String, Object>();
            map.put("id",""+i);
            map.put("description","商品描述"+i);
            map.put("brand","品牌"+i);
            map.put("name","商品名"+i);
            map.put("price", new Random(100).nextDouble());
            map.put("sale",i);
            map.put("timestamp", JSONObject.valueToString(new Date()));

            esData.setId(""+i);
            esData.setData(JSONObject.valueToString(map));

            datas.add(esData);

            if (i%1000==0) {
                elasticSearchServiceImpl.searchBulkIn(datas);
                datas.clear();
            }
        }
        if (datas.size() > 0) {
            elasticSearchServiceImpl.searchBulkIn(datas);
            datas.clear();
        }


    }
}