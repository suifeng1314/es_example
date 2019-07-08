package com.longtao.test;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ESTesst {
    // es client
    private TransportClient client;

    @Before
    public void getClient() throws UnknownHostException {
        // 连接集群名称
        Settings settings = Settings.builder().put("cluster.name","my-application").build();
        // 连接
        client = new PreBuiltTransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("bigdata4"),9300));

        // 输出
        System.out.println("client:" + client.toString());
    }

    @Test
    public void createIndex_blog(){
        // 创建索引
        client.admin().indices().prepareCreate("blog").get();
        // 关闭连接
        client.close();
    }

    @Test
    public void deleteIndex(){
        // 删除索引
        client.admin().indices().prepareDelete("blog3").get();
        // 关闭连接
        client.close();
    }

    /**
     * json方式
     */
    @Test
    public void createIndexByJson(){
        // 数据准备
        String json = "{" + "\"id\":\"1\"," + "\"title\":\"基于Lucene的搜索服务器\","
                + "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" + "}";
        // 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog","article","1").setSource(json).execute().actionGet();

        // 输出结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 关闭连接
        client.close();
    }

    /**
     * map方式（如果存在直接覆盖）
     */
    @Test
    public void createIndexByMap(){
        // 数据准备
        Map<String,Object> map = new HashMap<String, Object>();
        map.put("id",2);
        map.put("titile","基于Lucene的搜索服务器");
        map.put("content","它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口");

        // 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog","article","1").setSource(map).execute().actionGet();

        // 输出结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 关闭连接
        client.close();
    }

    /**
     * Xcontent 使用XContentBuilder创建document
     */
    @Test
    public void createIndexBXContent() throws IOException {
        // 通过es自带的帮助类，构建json数据
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("id",3);
        // 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog","artricle","3").setSource(builder).get();

        // 输出结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        // 关闭连接
        client.close();
    }

    /**
     * 查询文档
     */
    @Test
    public void getData(){
        // 查询文档
        GetResponse response = client.prepareGet("blog","article","1").get();
        // 输出结果 string
        System.out.println(response.getSourceAsString());
        // 关闭
        client.close();
    }

    /**
     * 查询多个文档
     */
    @Test
    public void getMultiData(){
        // 查询多个文档Multi
        MultiGetResponse responses = client.prepareMultiGet()
                .add("blog","article","1")
                .add("blog","article","2","3")
                .add("blog","article","2").get();

        for(MultiGetItemResponse itemResponse:responses){
            GetResponse getResponse = itemResponse.getResponse();

            // 是否有数据
            if (getResponse.isExists()){
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }
        client.close();
    }

    /**
     * 更新文档
     */
    @Test
    public void updateData() throws Throwable {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("blog");
        updateRequest.type("article");
        updateRequest.id("3");

        updateRequest.doc(XContentFactory.jsonBuilder().startObject()
                    // 没有字段添加，已有字段替换(覆盖)
                    .field("title","基于Lucene的搜索服务器")
                    .field("content","它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。大数据前景无限")
                    .field("createDate","2019-7-8").endObject());
        // 更新后的值
        UpdateResponse indexResponse = client.update(updateRequest).get();

        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("result:" + indexResponse.getResult());

        client.close();
    }

    /**
     * 按条件查询 查询不到添加
     */
    @Test
    public void testUpsert() throws Throwable {
        IndexRequest indexRequest = new IndexRequest("blog","article","5")
                .source(XContentFactory.jsonBuilder().startObject()
                        .field("titile","搜索服务器")
                        .field("content").endObject());
        UpdateRequest upsert = new UpdateRequest("blog","article","5")
                .doc(XContentFactory.jsonBuilder().startObject().field("user","lt").endObject()).upsert(indexRequest);

        client.update(upsert).get();
        client.close();
    }

    /**
     * 删除文档
     */
    @Test
    public void deleteData(){
        // 删除文档数据
        DeleteResponse indexResponse = client.prepareDelete("blog","article","5").get();

        // 2 打印返回的结果
        System.out.println("index:" + indexResponse.getIndex());
        System.out.println("type:" + indexResponse.getType());
        System.out.println("id:" + indexResponse.getId());
        System.out.println("version:" + indexResponse.getVersion());
        System.out.println("found:" + indexResponse.getResult());

        client.close();
    }

    /**
     * 查询多少个对象
     */
    @Test
    public void matchAllQuery(){

        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.matchAllQuery()).get();

        SearchHits hits = searchResponse.getHits();
        System.out.println("查询结果有：" + hits.getTotalHits() + "条");

        for (SearchHit hit: hits){
            System.out.println("结果：" + hit.getSourceAsString());
        }
        client.close();

    }

    /**
     * 条件查询
     */
    @Test
    public void query(){
        // condition search
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("大数据")).get();
        // 打印
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的结果：" + hits.getTotalHits() + "条");
        for (SearchHit hit: hits){
            System.out.println("结果：" + hit);
        }
        // 关闭
        client.close();
    }

    /**
     * 通配符查询
     */
    @Test
    public void wildcardQuery(){
        // 通配符查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.wildcardQuery("content","*全*")).get();
        // 获取命中次数 %大数据%
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的结果：" + hits.getTotalHits() + "条");
        for (SearchHit hit: hits){
            System.out.println("结果：" + hit);
        }
        // 关闭
        client.close();
    }

    /**
     * field查询
     */
    @Test
    public void termQuery(){
        // 类似于mysql =
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.termQuery("content","全文")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的结果：" + hits.getTotalHits() + "条");
        for (SearchHit hit: hits){
            System.out.println("结果：" + hit);
        }
        // 关闭
        client.close();
    }

    /**
     * 模糊查询
     */
    @Test
    public void fuzzy(){
        // 模糊查询
        SearchResponse searchResponse = client.prepareSearch("blog")
                .setTypes("article")
                .setQuery(QueryBuilders.fuzzyQuery("title","lucence")).get();
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询到的结果：" + hits.getTotalHits() + "条");
        Iterator<SearchHit> iterator = hits.iterator();

        while (iterator.hasNext()){
            SearchHit searchHit = iterator.next();// 每个查询对象
            System.out.println("结果：" + searchHit);
        }

        // 关闭
        client.close();
    }

    /**
     * 添加mapping
     */
    @Test
    public void createMapping() throws Exception {
        // 设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("article")
                .startObject("properties").startObject("id1").field("type","text").field("store","true").endObject()
                .startObject("titile2").field("type","text").field("store","false").endObject()
                .startObject("content").field("type","text").field("store","true").endObject().endObject().endObject().endObject();

        // 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest("blog4").type("article").source(builder);

        client.admin().indices().putMapping(mapping).get();
        client.close();
    }

    @Test
    public void createMappingIk() throws Exception {
        // 1设置mapping
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("article")
                .startObject("properties").startObject("id1").field("type", "text").field("store", "true")
                .field("analyzer", "ik_smart").endObject().startObject("title2").field("type", "text")
                .field("store", "false").field("analyzer", "ik_smart").endObject().startObject("content")
                .field("type", "text").field("store", "true").field("analyzer", "ik_smart").endObject().endObject()
                .endObject().endObject();

        // 添加mapping
        PutMappingRequest mapping = Requests.putMappingRequest("blog4").type("article").source(builder);

        client.admin().indices().putMapping(mapping).get();
        client.close();
    }

    /**
     * 词条查询
     */
    @Test
    public void queryTermForik(){
        // 分析结果 默认standard
        // analyzer分词器分词，大写字母全部转为了小写字母，并存入了倒排索引以供搜索。term是确切查询， 必须要匹配到大写的Name。
        SearchResponse response = client.prepareSearch("blog4").setTypes("article")
                .setQuery(QueryBuilders.termQuery("content","web")).get();
        // 获取结果
        SearchHits hits = response.getHits();

        System.out.println("结果条数：" + hits.getTotalHits());
        // 循环打印
        for (SearchHit hit : hits){
            System.out.println("结果：" + hit.getSourceAsString());
        }
    }

}
