package com.rainsoft.util.java;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2017-05-10.
 */
public class FileUtils {
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
    public static String convertFilContext(String path) throws IOException {


        //根据给出的目录创建文件对象
        File dir = new File(path);

        //列出目录下所有符合条件的文件
        File[] fileList = dir.listFiles();      //列出目录下所有的文件，包括目录

        /*
         * 遍历每一个文件，读出文件内容
         */
        for(File file : fileList){
            //读出所有文件的内容
            String context = org.apache.commons.io.FileUtils.readFileToString(file);
            context = context.replace("\r\n", "");
            context = context.replace("\n", "");
            context = context.replace("|$|", "|$|\r\n");
            org.apache.commons.io.FileUtils.writeStringToFile(file, context, false);

        }

        return path;
    }

    public static void main(String[] args) throws IOException {
        convertFilContext("D:\\0WorkSpace\\Develop\\data\\bcp\\im_chat");
    }
}
