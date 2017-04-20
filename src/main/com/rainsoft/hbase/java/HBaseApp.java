package com.rainsoft.hbase.java;

import com.rainsoft.util.java.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HBaseApp {


    /*
     * Get Data From Table By Rowkey
     */
    @Test
    public void getData() throws Exception {
        String tableName = "user";

        // Get table instance
        Table table = HBaseUtil.getTable(tableName);


        // Create Get with rowkey
        Get get = new Get(Bytes.toBytes("10005"));

        //=======================================
        get.addColumn(//
                Bytes.toBytes("info"),//
                Bytes.toBytes("age")//
        );

        //=======================================

        // Get Data
        Result result = table.get(get);

//		System.out.println(result);
        /**
         * Key:
         * 	rowkey + cf + c + version
         *
         * Value :
         * 	value
         */
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
            System.out.println("--------------------------------");
        }

        // close
        table.close();

    }

    /**
     * Put Data into Table
     *
     * @throws Exception Map<String,Object>
     */
    public void putData() throws Exception {
        String tableName = "user";

        // 获取表实例
        Table table = HBaseUtil.getTable(tableName);

        // 创建Put
        Put put = new Put(Bytes.toBytes("10001"));

        // 添加字段信息
        put.add(//
                Bytes.toBytes("info"), //
                Bytes.toBytes("sex"), //
                Bytes.toBytes("male") //
        );

        put.add(//
                Bytes.toBytes("info"), //
                Bytes.toBytes("tel"), //
                Bytes.toBytes("010-876523423") //
        );

        put.add(//
                Bytes.toBytes("info"), //
                Bytes.toBytes("address"), //
                Bytes.toBytes("beijing") //
        );
/*
        Map<String,Object> kvs = new HashMap<String,Object>() ;
		for(String key : kvs.keySet()){
			put.add(//
				HBaseTableConstant.HBASE_TABLE_USER_CF, //
				Bytes.toBytes(key), //
				Bytes.toBytes(kvs.get(key)) //
			) ;
		}
*/

        // put data into table
        table.put(put);

        // close
        table.close();
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

    public void scanData() throws Exception {
        String tableName = "user";

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

    public static void main(String[] args) throws Exception {
        String tableName = "user";
        new HBaseApp().scanData();

    }

}
