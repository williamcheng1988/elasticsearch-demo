package es.test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import es.ElasticSearchManger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;

public class EsApi {
    public static void main(String[] args) {
        ElasticSearchManger elasticSearchManger = new ElasticSearchManger();
        elasticSearchManger.init();
        testFact(elasticSearchManger.client);
    }

    public static void testFact(TransportClient client) {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("description", "商品描述7031");
        AbstractAggregationBuilder builder = AggregationBuilders.terms("sale_group").field("price");    //es高级查询agg
        AbstractAggregationBuilder sumAgg = AggregationBuilders.sum("sal_sum").field("sale");    //做分组

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("william-orders").setTypes("goods")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder)
                .setFrom(0).setSize(20)
                .addAggregation(builder).addAggregation(sumAgg);
        SearchResponse response = searchRequestBuilder.get();

        Terms terms = response.getAggregations().get("sale_group");


        InternalSum internalSum = (InternalSum) response.getAggregations().asMap().get("sal_sum");
        System.out.println("sal_sum====="+internalSum.getName() + ":" + internalSum.getValue());
        List<? extends Bucket> buckets = terms.getBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());
        }

    }

    public static void testQuery(TransportClient client) {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("description", "玉米");

        //也可以支持多个条件查询，如下：
        //QueryBuilder queryBuilder1 = QueryBuilders.termQuery("description", "连衣裙");
        //QueryBuilder queryBuilder2 = QueryBuilders.termQuery("price", "99");
        //QueryBuilder queryBuilder3 = QueryBuilders.rangeQuery("price").from(1).to(18);		//范围查询
        //QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(queryBuilder1).must(queryBuilder3);//设置多个组合查询


        // 高亮显示结果 非常重要的功能
        HighlightBuilder highlightBuilder = new HighlightBuilder().field(
                "description").requireFieldMatch(false);
        highlightBuilder.preTags("<p style='color:red'>");
        highlightBuilder.postTags("</p>");

        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch("william-orders").setTypes("goods")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(queryBuilder).setFrom(0).setSize(20);
        searchRequestBuilder.highlighter(highlightBuilder);

        SearchResponse response = searchRequestBuilder.get();
        System.out.println("查询总数:" + response.getHits().totalHits);
        for (SearchHit hit : response.getHits()) {
            Map<String, Object> map = hit.getSource();
            Map<String, HighlightField> map2 = hit.getHighlightFields();
            HighlightField field = map2.get("description");
            if (field != null) {
                Text[] texts = field.fragments();
                String content = "";
                for (Text text : texts) {
                    content += text;
                }
                map.put("description", content);
            }
            Iterator<String> iterator = map.keySet().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                System.out.print(key + ":" + map.get(key) + "	");
            }
            System.out.println();

        }
    }
}
