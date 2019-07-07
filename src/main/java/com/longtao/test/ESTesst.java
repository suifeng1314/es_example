package com.longtao.test;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

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
}
