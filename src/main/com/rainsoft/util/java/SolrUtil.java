package com.rainsoft.util.java;

import org.apache.solr.client.solrj.impl.CloudSolrClient;

/**
 * Created by Administrator on 2017-04-06.
 */
public class SolrUtil {

    //Solr的Zookeeper地址
    private static String zkHost = "dn1.hadoop.com,dn2.hadoop.com,dn3.hadoop.com,nn1.hadoop.com,nn2.hadoop.com";
    //Solr客户端
    private static CloudSolrClient client;

    /**
     * 获取Solr客户端连接
     * @param collection Solr的核心（集合）
     * @return Solr客户端连接
     */
    public static CloudSolrClient getSolrClient(String collection) {
        if (client == null) {
            client = new CloudSolrClient.Builder().withZkHost(zkHost).build();
        }
        return client;
    }
}
