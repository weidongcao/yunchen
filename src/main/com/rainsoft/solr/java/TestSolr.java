package com.rainsoft.solr.java;

import org.apache.solr.common.SolrInputDocument;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by Administrator on 2017-04-05.
 */
public class TestSolr {
    public static final String SOLR_URL = "http://nn1.hadoop.com:8080/solr/index.html";

    public static void main(String[] args) {
        System.out.println("Hello Solr");
    }

    public static void addDocs() {
        String[] words = {
                "中央全面深化改革领导小组", "第四次会议", "审议了国企薪酬制度改革", "考试招生制度改革",
                "传统媒体与新媒体融合等", "相关内容文件", "习近平强调要", "逐步规范国有企业收入分配秩序",
                "实现薪酬水平适当", "结构合理、管理规范、监督有效", "对不合理的偏高", "过高收入进行调整",
                "深化考试招生制度改革", "总的目标是形成分类考试", "综合评价", "多元录取的考试招生模式", "健全促进公平",
                "科学选才", "监督有力的体制机制", "着力打造一批形态多样", "手段先进", "具有竞争力的新型主流媒体",
                "建成几家拥有强大实力和传播力", "公信力", "影响力的新型媒体集团"
        };

        long start = System.currentTimeMillis();

        Collection<SolrInputDocument> docs = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            SolrInputDocument doc = new SolrInputDocument();

            doc.addField("id", "id" + i, 1.0f);
            doc.addField("name", words[i % 21], 1.0f);
            doc.addField("price", 10 * i);

            docs.add(doc);
        }
    }

}
