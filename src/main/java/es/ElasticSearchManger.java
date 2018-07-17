package es;

import java.io.IOException;
import java.net.InetAddress;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

public class ElasticSearchManger {
	public TransportClient client;
    Settings settings;

    public ElasticSearchManger() {
        init();
    }

    public void init() {
        //cluster.name 集群名称
        //client.transport.sniff:true 使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中，这样做的好处是一般你不用手动设置集群里所有集群的ip到连接客户端，它会自动帮你添加，并且自动发现新加入集群的机器
        settings = Settings.builder().put("cluster.name", "william-es").put("client.transport.sniff", true)
                .build();
        try {
            //设置集群中节点的 IP 端口
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.86.128"), 9300))
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.86.128"), 8300));
                    //.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.0.21"), 7300));
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    /*public static void main(String[] args) {
        ElasticSearchManger elasticSearchManger = new ElasticSearchManger();
        elasticSearchManger.init();
        SearchResponse response = elasticSearchManger.client.prepareSearch("william-orders").setTypes("item_loc")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.queryStringQuery("item_desc:格力"))
                .setFrom(0).setSize(10).setExplain(false)
                .get();
        System.out.println(response);
    }*/
}
