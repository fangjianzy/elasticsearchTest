package com.kiso.mq.elasticsearchTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.JsonProcessingException;

public class EsTest {
	//客户端
	TransportClient transportClient;  
    //索引库名  
    String index = "goods";  
    //类型名称  
    String type = "product";  
    //搜索数据并且返回,一般带.get()
    GetResponse response;
    //初始化
    @Before  
    public void before() throws UnknownHostException {  
    	 //设置集群名称
        Settings settings = Settings.builder()
       		 .put("client.transport.sniff", true)//这个客户端可以嗅到集群的其它部分，并将它们加入到机器列表。为了开启该功能，设置client.transport.sniff为true。
             .put("cluster.name", "my-application")
             .build();
        //创建client
        transportClient = new PreBuiltTransportClient(settings).addTransportAddress(
        		new InetSocketTransportAddress(InetAddress.getByName("192.168.8.8"), 9300));
    } 
    
    @After  
    public void After() throws UnknownHostException {
    	transportClient.close();
    } 
    
    /** 
     * 通过prepareGet方法获取指定文档信息 
     */  
    @Test  
    public void testGetIndex() {  
        GetResponse getResponse = transportClient.prepareGet(index, type, "2").get();  
        System.out.println(getResponse.getSourceAsString());  
    }
    
   
    
    /** 
     * 通过prepareIndex增加文档，参数为json字符串 
     */  
    @Test  
    public void testAddIndexJson()  
    {  
        String source = "{\"name\":\"zhangyan\",\"age\":18}";  
        IndexResponse indexResponse = transportClient  
                .prepareIndex(index, type, "12").setSource(source).get();  
        System.out.println(indexResponse.getVersion());  
    }
    
    /** 
     * 通过prepareIndex增加文档，参数为Map<String,Object> 
     */  
    @Test  
    public void testAddIndexMap()  
    {  
        Map<String, Object> source = new HashMap<String, Object>(2);  
        source.put("name", "waihaha");  
        source.put("age", 28);  
        IndexResponse indexResponse = transportClient  
                .prepareIndex(index, type).setSource(source).get();  
        System.out.println(indexResponse.getVersion());  
    }
    /**
     * 不指定ID情况下，默认根据哈希值的ID
     */
    @Test  
    public void testAddIndexBean() throws ElasticsearchException, JsonProcessingException  
    {  
        Student stu = new Student();  
        stu.setName("jsonBean");  
        stu.setAge(22);  
        stu.setLyrics("测试中文");
        stu.setYear(2010);
        IndexResponse indexResponse = transportClient  
                .prepareIndex(index, type).setSource(JSON.toJSONString(stu)).get();  
        System.out.println(indexResponse.getId());  
    }
    
    /** 
     * 通过prepareIndex增加文档，参数为XContentBuilder,ES自带的json工具
     *  
     * @throws IOException 
     * @throws InterruptedException 
     * @throws ExecutionException 
     */  
    @Test  
    public void testAddEsJson() throws IOException, InterruptedException, ExecutionException  
    {  
        XContentBuilder builder = XContentFactory.jsonBuilder()  
                .startObject()  
                .field("name", "Avivi")  
                .field("age", 30)  
                .endObject();  
        IndexResponse indexResponse = transportClient  
                .prepareIndex(index, type, "6")  
                .setSource(builder)  
                .execute().get();  
        //.execute().get();和get()效果一样  
        System.out.println(indexResponse.getVersion());  
    }  
    
    /** 
     * prepareUpdate更新索引库中文档，如果文档不存在则会报错 
     * @throws IOException 
     *  
     */  
    @Test  
    public void testUpdate() throws IOException  
    {  
        XContentBuilder source = XContentFactory.jsonBuilder()  
            .startObject()  
            .field("name", "will")  
            .field("year", "1986")
            .endObject();  
          
        UpdateResponse updateResponse = transportClient  
                .prepareUpdate(index, type, "2").setDoc(source).get();  
          
        System.out.println(updateResponse.getVersion());  
    }
    
    /**
     * 更新对象
     * @throws IOException
     */
    @Test  
    public void testUpdateResponse() throws IOException  
    {  
    	String id = "3";
    	UpdateRequest updaterequest = new UpdateRequest(index, type, id);
    	UpdateResponse updateresponse = null;
    	Student stu = new Student();  
        stu.setName("jsonBean");  
        stu.setAge(22);  
        stu.setLyrics("测试中文11123213");
        stu.setYear(2001);
    	updaterequest.doc(JSON.toJSONString(stu));
    	try {
			updateresponse = transportClient.update(updaterequest).get();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println(updateresponse);
    }
    
    
    /** 
     * 通过prepareDelete删除文档 
     *  
     */  
    @Test  
    public void testDelete()  
    {  
        String id = "2";  
        DeleteResponse deleteResponse = transportClient.prepareDelete(index,type, id).get();  
        System.out.println(deleteResponse.getResult());  
        transportClient.prepareDelete(index, type, id);
    }
    
    /** 
     * 删除索引库，不可逆慎用 
     */  
    //@Test  
    public void testDeleteeIndex()  
    {  
        transportClient.admin().indices().prepareDelete("shb01","shb02").get();  
    } 
    
    
    @Test  
    public void testCount()  
    {  
        long count = transportClient.filteredNodes().size();
        		
        System.out.println(count);  
    }
    
    /** 
     * 通过prepareBulk执行批处理 
     *  
     * @throws IOException  
     */  
    @Test      
    
    
    
    
    
    
    public void testBulk() throws IOException  
    {  
        //1:生成bulk  
        BulkRequestBuilder bulk = transportClient.prepareBulk();  
          
        //2:新增  
        IndexRequest add = new IndexRequest(index, type, "10");  
        add.source(XContentFactory.jsonBuilder()  
                    .startObject()  
                    .field("name", "Henrry").field("age", 30)  
                    .endObject());  
          
        //3:删除  
        DeleteRequest del = new DeleteRequest(index, type, "1");  
          
        //4:修改  
        XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("name", "jack_1").field("age", 19).endObject();  
        UpdateRequest update = new UpdateRequest(index, type, "2");  
        update.doc(source);  
          
        bulk.add(del);  
        bulk.add(add);  
        bulk.add(update);  
        //5:执行批处理  
        BulkResponse bulkResponse = bulk.get();  
        if(bulkResponse.hasFailures())  
        {  
            BulkItemResponse[] items = bulkResponse.getItems();  
            for(BulkItemResponse item : items)  
            {  
                System.out.println(item.getFailureMessage());  
            }  
        }  
        else  
        {  
            System.out.println("全部执行成功！");  
        }  
    }
    
    /**
     * ES 的查询
     */
    @Test  
    public void testSearch()  
    {  
    	String[] fields = {"goodsName","id","orderNum","channelId","minPrice",
        		"goodStatus","saleNum","qualitySort","score"};
    	
        SearchResponse searchResponse = transportClient.prepareSearch(index).setTypes(type)
        		.setFetchSource(fields,null)
                //.setQuery(QueryBuilders.matchAllQuery()) //查询所有  
                //.setQuery(QueryBuilders.matchQuery("name", "will").operator(Operator.AND)) //根据tom分词查询name,默认or  
                //.setQuery(QueryBuilders.multiMatchQuery("will", "name", "18","age")) //指定查询的字段  
                //.setQuery(QueryBuilders.queryStringQuery("name:*i* AND age:[0 TO 40]")) //根据条件查询,支持通配符大于等于0小于等于19  
                .setQuery(QueryBuilders.queryStringQuery("goodsName:*背心* and goodStatus:6 and channelId:11470624249826"))
        		//.setQuery(QueryBuilders.matchQuery("goodsName", "工字背心"))//单个查询1
                //.setQuery(QueryBuilders.matchQuery("channelId", "11470624249826"))//单个查询2
                //.setQuery(QueryBuilders.matchQuery("goodStatus", "6"))//单个查询3
                //.setQuery(QueryBuilders.matchQuery("regionIds", "6"))//单个查询2
                //.setQuery(QueryBuilders.termQuery("name", "will"))//查询时不分词  ,准确查找类似于and
                .setSearchType(SearchType.QUERY_THEN_FETCH)  
                .setFrom(0).setSize(10)//分页  
                .addSort("minPrice", SortOrder.ASC)//排序   asc升序，desc降序
                .get();  
          
        SearchHits hits = searchResponse.getHits();  
        long total = hits.getTotalHits();  
        System.out.println(total);  
        SearchHit[] searchHits = hits.hits();  
        for(SearchHit s : searchHits)  
        {  
            System.out.println(s.getSourceAsString());  
        }  
    } 
    /**
     * must: and
     * mutnot: not
     * should: or
     */
    @Test 
    public void testSearchMore() 
    { 
    	String[] fields = {"goodsName","id","orderNum","channelId","minPrice",
        		"goodStatus","saleNum","qualitySort","score"};
        SearchResponse searchResponse = transportClient.prepareSearch(index) 
                .setTypes(type)
                .setFetchSource(fields, null)
                //.setQuery(QueryBuilders.matchAllQuery()) //查询所有
                .setQuery(QueryBuilders.boolQuery()
                		.must(QueryBuilders.multiMatchQuery("工字背心","goodsName", "companName","brandName"))
                		//.must(QueryBuilders.matchQuery("brandName", "工字背心"))
                		//.must(QueryBuilders.matchQuery("companyName", "工字背心"))
                		
                		.mustNot(QueryBuilders.matchQuery("regionIds", "2260"))
                		.must(QueryBuilders.matchQuery("channelId", "11470624249826"))
                		.must(QueryBuilders.matchQuery("goodStatus", "6"))
                		.should(QueryBuilders.matchQuery("channelId", "11470624249800")))
                		//.filter(QueryBuilders.matchQuery("goodStatus", "6")))//查询频道
                //.setQuery(QueryBuilders.matchQuery("channelId", "11470624249826").operator(Operator.OR)) //根据tom分词查询name,默认or
                //.setQuery(QueryBuilders.matchQuery("channelId", "11470624249826")) //根据tom分词查询name,默认or
                //.setQuery(QueryBuilders.queryString("name:to* AND age:[0 TO 19]")) //根据条件查询,支持通配符大于等于0小于等于19 
                //.setQuery(QueryBuilders.termQuery("goodStatus", "6"))//查询时不分词 
                .setSearchType(SearchType.QUERY_THEN_FETCH) 
                .setFrom(0).setSize(10)//分页 
                .addSort("minPrice", SortOrder.DESC)//排序 
                .addSort("orderNum", SortOrder.ASC)
                .get(); 
           
        SearchHits hits = searchResponse.getHits(); 
        long total = hits.getTotalHits(); 
        System.out.println(total); 
        SearchHit[] searchHits = hits.hits(); 
        for(SearchHit s : searchHits) 
        { 
            System.out.println(s.getSourceAsString());
            String []logindex=s.getSourceAsString().split(",");
             
        } 
    } 
    /** 
     * 多索引，多类型查询 
     * timeout 
     */  
    @Test  
    public void testSearchsAndTimeout()  
    {  
    	SearchResponse searchResponse = transportClient.prepareSearch(index,".kibana").setTypes(type,"config")    
            .setQuery(QueryBuilders.matchAllQuery())  
            .setSearchType(SearchType.QUERY_THEN_FETCH) 
            .get();  
          
        SearchHits hits = searchResponse.getHits();  
        long totalHits = hits.getTotalHits();  
        System.out.println(totalHits);  
        SearchHit[] hits2 = hits.getHits();  
        for(SearchHit h : hits2)  
        {  
            System.out.println(h.getSourceAsString());  
        }  
    } 
    
    /** 
     * 过滤， 
     * lt 小于 
     * gt 大于 
     * lte 小于等于 
     * gte 大于等于 
     *  
     */  
    @Test  
    public void testFilter()  
    {  
        SearchResponse searchResponse = transportClient.prepareSearch(index)  
                .setTypes(type)  
                .setQuery(QueryBuilders.matchAllQuery()) //查询所有  
                .setSearchType(SearchType.QUERY_THEN_FETCH) 
                
//              .setPostFilter(FilterBuilders.rangeFilter("age").from(0).to(19)  
//                      .includeLower(true).includeUpper(true))  
                .setExplain(true) //explain为true表示根据数据相关度排序，和关键字匹配最高的排在前面  
                .get();  
      
          
        SearchHits hits = searchResponse.getHits();  
        long total = hits.getTotalHits();  
        System.out.println(total);  
        SearchHit[] searchHits = hits.hits();  
        for(SearchHit s : searchHits)  
        {  
            System.out.println(s.getSourceAsString());  
        }  
    }
    
    /** 
     * 高亮 
     */  
    @Test  
    public void testHighLight()  
    {  
        SearchResponse searchResponse = transportClient.prepareSearch(index)  
                .setTypes(type)  
                //.setQuery(QueryBuilders.matchQuery("name", "Fresh")) //查询所有  
                .setQuery(QueryBuilders.queryStringQuery("name:F*"))  
                .setSearchType(SearchType.QUERY_THEN_FETCH)  
                .get();  
      
          
        SearchHits hits = searchResponse.getHits();  
        System.out.println("sum:" + hits.getTotalHits());  
          
        SearchHit[] hits2 = hits.getHits();  
        for(SearchHit s : hits2)  
            System.out.println(s.getSourceAsString());  
    } 
    
    /** 
     * 分组 
     */  
    @Test  
    public void testGroupBy()  
    {  
        SearchResponse searchResponse = transportClient.prepareSearch(index).setTypes(type)  
                .setQuery(QueryBuilders.matchAllQuery())  
                .setSearchType(SearchType.QUERY_THEN_FETCH)  
                .addAggregation(AggregationBuilders.terms("group_age")  
                        .field("age").size(0))//根据age分组，默认返回10，size(0)也是10  
                .get();  
    } 
    
    /** 
     * 聚合函数,本例之编写了sum，其他的聚合函数也可以实现 
     *  
     */  
    @Test  
    public void testMethod()  
    {  
        SearchResponse searchResponse = transportClient.prepareSearch(index).setTypes(type)  
                .setQuery(QueryBuilders.matchAllQuery())  
                .setSearchType(SearchType.QUERY_THEN_FETCH)  
                .addAggregation(AggregationBuilders.terms("group_name").field("name")  
                        .subAggregation(AggregationBuilders.sum("sum_age").field("age")))  
                .get();  
          
        Terms terms = searchResponse.getAggregations().get("group_name");  
        List<Bucket> buckets = terms.getBuckets();  
        for(Bucket bt : buckets)  
        {  
            Sum sum = bt.getAggregations().get("sum_age");  
            System.out.println(bt.getKey() + "  " + bt.getDocCount() + " "+ sum.getValue());  
        }  
          
    }  
}
