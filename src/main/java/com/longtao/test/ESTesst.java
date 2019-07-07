package com.longtao.test;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

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
}
