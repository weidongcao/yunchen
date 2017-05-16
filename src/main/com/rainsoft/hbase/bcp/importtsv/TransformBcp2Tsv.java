package com.rainsoft.hbase.bcp.importtsv;

import com.rainsoft.manager.ConfManager;
import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.PropConstants;
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
public class TransformBcp2Tsv {

    public static void main(String[] args) throws IOException, ParseException {
        String inputPath = args[0];
        String outputPath = args[1];
        createTsv(inputPath, outputPath, 27, "http", 17);

    }

    /**
     * 将BCP文件过滤、合并、转换为TSV文件
     *
     * @param inputPath
     * @param outputPath
     * @return
     * @throws IOException
     * @throws ParseException
     */
    public static String createTsv(String inputPath, String outputPath, int columnLength, String type, int capTimeIndex) throws IOException, ParseException {
        //BCP文件所在目录
        File dir = new File(inputPath);

        //文件最大行数
        int maxLine = ConfManager.getInteger(PropConstants.FILE_MAX_LINE);
        //统计的行数
        int lineNum = 0;
        //文件个数
        int fileNum = 0;

        DateFormat dateFormat = new SimpleDateFormat("yyMMddHHmm");

        //转换后生成的文件
        File httpTsvFile = new File(outputPath + "/" + type + "_" + dateFormat.format(new Date()) + "_" + fileNum + ".tsv");
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
                    //采集时间
                    String capTime = arr[capTimeIndex];

                    //行数加1(用于文件拆分)
                    lineNum++;
                    //采集时间不能为空
                    if (DateUtils.isDate(capTime, DateUtils.TIME_FORMAT) == true) {
                        //HBase的rowkey以采集时间的毫秒数加上一个随机数
                        long cap = DateUtils.TIME_FORMAT.parse(capTime).getTime();
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
            }

            /**
             * 限制文件行数，如果文件行数超过最大行数则写入新的文件
             */
            if (lineNum >= maxLine) {
                lineNum = 0;
                fileNum++;

                httpTsvFile = new File(outputPath + "/" + type + "_" + dateFormat.format(new Date()) + "_" + fileNum + ".tsv");
                httpTsvFile.createNewFile();
            }


            FileUtils.writeStringToFile(httpTsvFile, sb.toString(), true);
            fr.close();
        }

        return null;
    }
}
