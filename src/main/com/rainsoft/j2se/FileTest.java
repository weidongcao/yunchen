package com.rainsoft.j2se;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Administrator on 2017-04-26.
 */
public class FileTest {
    public static void main(String[] args) throws IOException {
//        File file = new File(FileTest.class.getClassLoader().getResource("resource/sql/AreaCollisionJoin.sql").getPath());
//        String str = FileUtils.readFileToString(file);
        InputStream in = FileTest.class.getClassLoader().getResourceAsStream("sql/AreaCollisionJoin.sql");
        String str = IOUtils.toString(in);
        System.out.println(str);
        IOUtils.closeQuietly(in);

    }
    public void run() {
        System.out.println(this.getClass().getResource(""));
        System.out.println(this.getClass().getResource("/"));
        System.out.println(this.getClass().getClassLoader().getResource("sql/AreaCollisionJoin.sql"));
    }
}
