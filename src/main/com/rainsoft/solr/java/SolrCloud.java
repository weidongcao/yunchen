package com.rainsoft.solr.java;

import com.rainsoft.domain.java.User;
import com.rainsoft.util.java.SolrUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017-04-06.
 */
public class SolrCloud {
    //获取客户端连接
    private static CloudSolrClient client = SolrUtil.getSolrClient("test");

    public static void main(String[] args) throws IOException, SolrServerException {

        List<HashMap<String, Object>> datasList = new ArrayList<>();
        HashMap<String, Object> tmp1 = new HashMap<>();
        tmp1.put("name", "ZhuXiaomeng");
        tmp1.put("age", 28);
        tmp1.put("city", "ShangHai");

        HashMap<String, Object> tmp2 = new HashMap<>();
        tmp2.put("name", "ZhangErjun");
        tmp2.put("age", 38);
        tmp2.put("city", "BeiJing");
        HashMap<String, Object> tmp3 = new HashMap<>();
        tmp3.put("name", "CaoWeidong");
        tmp3.put("age", 27);
        tmp3.put("city", "ShenZhen");
        datasList.add(tmp1);
        datasList.add(tmp2);
        datasList.add(tmp3);

//        insertSolr(datasList);
//        delSolr("*:*");
        querySolr(client);
    }

    public static boolean insertSolr(List<HashMap<String, Object>> datasList) {

        List<SolrInputDocument> colsList = new ArrayList<>();

        for (HashMap<String, Object> cols : datasList) {
            //创建插入对象
            SolrInputDocument document = new SolrInputDocument();

            for (Map.Entry<String, Object> entry : cols.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                //向插入对象中添加字段及字段值
                System.out.println(key + " --> " + value);
                document.addField(key, value);
            }

            colsList.add(document);
        }

        try {
            //将插入对象值给客户端连接
            client.add(colsList);
            //提交请求
            client.commit();
            return true;
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    public static boolean delSolr(String query) {

        try {
            client.deleteByQuery(query);
            client.commit();
            return true;
        } catch (SolrServerException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return false;
    }

    public static void querySolr(SolrClient client) throws IOException, SolrServerException {
        SolrQuery query = new SolrQuery();
        //查询条件为包含“12”的关键字
        query.set("q", "CaoWeidong");
        //查询类型为查询
        query.set("qt", "/select");
        //查询集合为“hbase”
        query.set("collection", "test");

        //客户端提交
        QueryResponse response = client.query(query, SolrRequest.METHOD.POST);

        //获取返回结果
        SolrDocumentList docList = response.getResults();

        System.out.println("文档个数： " + docList.getNumFound());
        System.out.println("查询时间： " + response.getQTime());
    }
}