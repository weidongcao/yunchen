package com.rainsoft.hbase.bcp.importtsv;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by Administrator on 2017-05-12.
 */
public class createBbsTSV {
    public static void main(String[] args) throws IOException, ParseException {
        String inputPath = args[0];
        String outputPath = args[1];
        TransformBcp2Tsv.createTsv(inputPath, outputPath, 25, "bbs", 24);
    }
}
