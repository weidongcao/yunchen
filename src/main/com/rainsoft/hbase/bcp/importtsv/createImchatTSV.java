package com.rainsoft.hbase.bcp.importtsv;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by Administrator on 2017-05-12.
 */
public class createImchatTSV {
    public static void main(String[] args) throws IOException, ParseException {
        String inputPath = args[0];
        String outputPath = args[1];

        //处理聊天内容的换行问题
        com.rainsoft.util.java.FileUtils.convertFilContext(inputPath);

        TransformBcp2Tsv.createTsv(inputPath, outputPath, 25, "im_chat", 20);
    }
}
