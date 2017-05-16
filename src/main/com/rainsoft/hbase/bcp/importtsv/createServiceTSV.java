package com.rainsoft.hbase.bcp.importtsv;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

/**
 * Created by Administrator on 2017-05-12.
 */
public class createServiceTSV {
    public static void main(String[] args) throws IOException, ParseException {
        String inputPath = args[0];
        String outputPath = args[1];
        createServiceTsv(inputPath, outputPath, 46, "service");
    }

    public static String createServiceTsv(String inputPath, String outputPath, int columnLength, String type) throws IOException, ParseException {
        //BCP文件所在目录
        File dir = new File(inputPath);

        DateFormat dateFormat = new SimpleDateFormat("yyMMddHHmm");
        //转换后生成的文件
        File httpTsvFile = new File(outputPath + "/" + type + "_" + dateFormat.format(new Date()) + ".tsv");
        if (httpTsvFile.exists()) {
            httpTsvFile.delete();
        }
        //创建要生成的TSV文件
        httpTsvFile.createNewFile();

        //列出目录下所有符合条件的文件
        File[] fileList = dir.listFiles();      //列出目录下所有的文件，包括目录

        /*
         * 遍历每一个文件，读出文件内容
         */
        for (File file : fileList) {

            FileReader fr = new FileReader(file);

            BufferedReader br = new BufferedReader(fr);

            StringBuilder sb = new StringBuilder();
            String line;

            //读取文件内容并过滤
            while ((line = br.readLine()) != null) {
                String[] arr = line.replace("|$|", "").split("\\|#\\|");
                if (arr.length >= columnLength) {
                    //采集时间不能为空
                    //HBase的rowkey以采集时间的毫秒数加上一个随机数
                    long cap = new Date().getTime();
                    //生成随机数
                    String uuid = UUID.randomUUID().toString().replace("-", "").substring(16);

                    //截取需要写入HBase的字段
                    String[] temp = Arrays.copyOfRange(arr, 0, columnLength);
                    //Hbase的Rowkey
                    String rowkey = cap + uuid;

                    //以制表符分隔
                    String field = rowkey + "\t" + StringUtils.join(temp, "\t") + "\r\n";

                    sb.append(field);
                }
            }
            fr.close();
            FileUtils.writeStringToFile(httpTsvFile, sb.toString(), true);
        }

        return null;
    }
}
