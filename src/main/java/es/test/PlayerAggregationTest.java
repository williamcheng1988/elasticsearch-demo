package es.test;

import es.ElasticSearchManger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.InternalAvg;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;

import java.util.Iterator;
import java.util.Map;

/**
 * User: William Cheng
 * Create Time: 2018/7/15 23:20
 * Description:
 */
public class PlayerAggregationTest {


    public static void main(String[] args) {
        ElasticSearchManger elasticSearchManger = new ElasticSearchManger();

        //首先，初始化Builder：
        SearchRequestBuilder sbuilder = elasticSearchManger.client.prepareSearch("players-normal");

        SearchResponse response = null;

        /*{
            //group by/count
            //例如要计算每个球队的球员数，如果使用SQL语句，应表达如下：
            //select team, count(*) as player_count from player group by team;
            //ES的java api：
            TermsAggregationBuilder teamAgg = AggregationBuilders.terms("player_count").field("team");
            sbuilder.addAggregation(teamAgg);
            response = sbuilder.execute().actionGet();
        }*/
        /*{
            //group by多个field
            //例如要计算每个球队每个位置的球员数，如果使用SQL语句，应表达如下：
            //select team, position, count(*) as pos_count from player group by team, position;
            //ES的java api：
            TermsAggregationBuilder teamAgg = AggregationBuilders.terms("player_count").field("team");
            TermsAggregationBuilder posAgg = AggregationBuilders.terms("pos_count").field("position");
            sbuilder.addAggregation(teamAgg.subAggregation(posAgg));
            response = sbuilder.execute().actionGet();
        }*/
        {
            //max/min/sum/avg
            //例如要计算每个球队年龄最大/最小/总/平均的球员年龄，如果使用SQL语句，应表达如下：
            //select team, max(age) as max_age from player group by team;
            //ES的java api：
            TermsAggregationBuilder teamAgg = AggregationBuilders.terms("player_count").field("team");
            MaxAggregationBuilder ageMax = AggregationBuilders.max("max_age").field("age");
            AvgAggregationBuilder ageAvg = AggregationBuilders.avg("avg_age").field("age");
            SumAggregationBuilder salaryAgg = AggregationBuilders.sum("total_salary").field("salary");
            sbuilder.addAggregation(teamAgg.subAggregation(ageMax).subAggregation(ageAvg).subAggregation(salaryAgg));
            response = sbuilder.execute().actionGet();
        }
        /*{
            //对多个field求max/min/sum/avg
            //例如要计算每个球队球员的平均年龄，同时又要计算总年薪，如果使用SQL语句，应表达如下：
            //select team, avg(age)as avg_age, sum(salary) as total_salary from player group by team;
            //ES的java api：
            TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team");
            AvgAggregationBuilder ageAgg = AggregationBuilders.avg("avg_age").field("age");
            SumAggregationBuilder salaryAgg = AggregationBuilders.sum("total_salary").field("salary");
            sbuilder.addAggregation(teamAgg.subAggregation(ageAgg).subAggregation(salaryAgg));
            response = sbuilder.execute().actionGet();
        }
        {
            //聚合后对Aggregation结果排序
            //例如要计算每个球队总年薪，并按照总年薪倒序排列，如果使用SQL语句，应表达如下：
            //select team, sum(salary) as total_salary from player group by team order by total_salary desc;
            //ES的java api：
            TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team").order(Terms.Order.aggregation("total_salary ", false));
            SumAggregationBuilder salaryAgg = AggregationBuilders.sum("total_salary").field("salary");
            sbuilder.addAggregation(teamAgg.subAggregation(salaryAgg));
            response = sbuilder.execute().actionGet();
        }
        {
            //需要特别注意的是，排序是在TermAggregation处执行的，Order.aggregation函数的第一个参数是aggregation的名字，第二个参数是boolean型，true表示正序，false表示倒序。
            //Aggregation结果条数的问题
            //默认情况下，search执行后，仅返回10条聚合结果，如果想反悔更多的结果，需要在构建TermsBuilder 时指定size：
            TermsAggregationBuilder teamAgg = AggregationBuilders.terms("team").size(15);
        }*/
        /*{
            //高亮显示结果 非常重要的功能
            HighlightBuilder highlightBuilder = new HighlightBuilder()
                    .field("team").requireFieldMatch(false)
                    .preTags("<p style='color:red'>").postTags("</p>");
            sbuilder.highlighter(highlightBuilder);
        }*/
        print(response);
    }

    private static void print(SearchResponse response) {
        //Aggregation结果的解析/输出
        //得到response后：
        Map<String, Aggregation> aggMap = response.getAggregations().asMap();
        StringTerms teamAgg = (StringTerms) aggMap.get("player_count");
        Iterator<? extends Terms.Bucket> teamBucketIt = teamAgg.getBuckets().iterator();
        while (teamBucketIt.hasNext()) {
            Terms.Bucket buck = teamBucketIt.next();
            System.out.println(buck.getKey() + ":" + buck.getDocCount());
            //球队名
            String team = buck.getKey() + "";
            //记录数
            long count = buck.getDocCount();
            //得到所有子聚合
            Map subaggmap = buck.getAggregations().asMap();
            //max值获取方法
            double max_age = ((InternalMax) subaggmap.get("max_age")).getValue();
            System.out.println("max值获取方法:" + max_age);
            //avg值获取方法
            double avg_age = ((InternalAvg) subaggmap.get("avg_age")).getValue();
            System.out.println("avg值获取方法:" + avg_age);
            //sum值获取方法
            double total_salary = ((InternalSum) subaggmap.get("total_salary")).getValue();
            System.out.println("sum值获取方法:" + total_salary);
            //...
            //max/min以此类推
        }
    }
}