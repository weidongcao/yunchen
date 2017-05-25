package com.rainsoft.hbase.java;

import com.rainsoft.util.java.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HBaseApp {
    public static void main(String[] args) throws Exception {
        HBaseApp app = new HBaseApp();
        app.scanData("user");

    }
    public void deleteData() throws Exception {
        String tableName = "user";

        // Get table instance
        Table table = HBaseUtil.getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes("10005"));

        delete.deleteColumn(//
                Bytes.toBytes("info"), //
                Bytes.toBytes("sex") //
        );

        table.delete(delete);

        // close
        table.close();
    }

    public void scanData(String tableName) throws Exception {
        // Get table instance
        Table table = null;
        ResultScanner resultScanner = null;

        try {
            table = HBaseUtil.getTable(tableName);

            Scan scan = new Scan();
            resultScanner = table.getScanner(scan);

            // iterator
            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println(//
                            Bytes.toString(CellUtil.cloneFamily(cell))
                                    + ":" //
                                    + Bytes.toString(CellUtil.cloneQualifier(cell)) //
                                    + " -> " //
                                    + Bytes.toString(CellUtil.cloneValue(cell)) //
                                    + " " //
                                    + cell.getTimestamp()
                    );
                }
                System.out.println("--------------------------------");
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(resultScanner);
            IOUtils.closeStream(table);
        }

    }



}
