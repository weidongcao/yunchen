package com.rainsoft.hbase.bcp.importtsv;

import com.rainsoft.util.java.DateUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.UUID;

/**
 * Created by Administrator on 2017-05-11.
 */
public class CreateHttpTsv {
    public static void main(String[] args) {

    }

    public static String createHttpTsvJob(String path) throws IOException, ParseException {
        File dir = new File(path);

        File httpTsvFile = new File("http.tsv");
        if (httpTsvFile.exists()) {
            httpTsvFile.delete();
        }
        httpTsvFile.createNewFile();

        StringBuilder sb = new StringBuilder();

        //列出目录下所有符合条件的文件
        File[] fileList = dir.listFiles();      //列出目录下所有的文件，包括目录

        /*
         * 遍历每一个文件，读出文件内容
         */
        for (File file : fileList) {

            FileReader fr = new FileReader(file);

            BufferedReader br = new BufferedReader(fr);
            String line;
            while ((line = br.readLine()) != null) {
                String[] arr = line.replace("\\|$\\|", "").split("\\|#\\|");
                if (arr.length >= 27) {
                    String certificate_code = arr[1];
                    String capTime = arr[22];
                    if ((null != certificate_code)
                            && ("".equals(certificate_code) == false)
                            && (DateUtils.isDate(capTime, DateUtils.TIME_FORMAT) == true)) {
                        long cap = DateUtils.TIME_FORMAT.parse(capTime).getTime();
                        String uuid = UUID.randomUUID().toString().replace("-", "");
                        String rowkey = cap + certificate_code + uuid.substring(20);
                        String[] temp = Arrays.copyOfRange(arr, 0, 27);
                        String field = rowkey + "\t" + StringUtils.join(temp, "\t") + "\r\n";
                        sb.append(field);
                    }
                }
            }
            //读出所有文件的内容
            String context = org.apache.commons.io.FileUtils.readFileToString(file);
            context = context.replace("\r\n", "");
            context = context.replace("\n", "");
            context = context.replace("|$|", "|$|\r\n");
        }
//        org.apache.commons.io.FileUtils.writeStringToFile(file, context, false);

        return null;
    }
}
