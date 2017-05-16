package com.rainsoft.hbase.bcp.importtsv;

import com.rainsoft.util.java.DateUtils;
import com.rainsoft.util.java.FieldConstant;
import com.rainsoft.util.java.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.text.ParseException;
import java.util.UUID;

import static com.rainsoft.util.java.DateUtils.TIME_FORMAT;

public class CreateHttpHFile extends Configured implements Tool {

    public static final String cf = "CONTENT_HTTP";
    public static final String[] columns = FieldConstant.HBASE_FIELD_MAP.get("http");

    public static class TransformHFileMapper extends
            Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        //, ImmutableBytesWritable, KeyValue
        private ImmutableBytesWritable rowkey = new ImmutableBytesWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //line value
            String line = value.toString().replace("\\|$\\|", "");

            //split
            String[] arr = line.split("\\|#\\|");

            if (arr.length < columns.length) {
                return;
            }

            String certificate_code = arr[1];
            String capTime = arr[22];
            if ((null != certificate_code)
                    || ("".equals(certificate_code) == false)) {
                return;
            }
            if (DateUtils.isDate(capTime, TIME_FORMAT) == false) {
                return;
            }
            long cap;
            try {
                cap = TIME_FORMAT.parse(capTime).getTime();
            } catch (ParseException e) {
                return;
            }

            String uuid = UUID.randomUUID().toString().replace("-", "");
            String rowkey_string = cap + certificate_code + uuid.substring(16);

            //create rowkey
            rowkey.set(Bytes.toBytes(rowkey_string));

            //create put instance
            Put put = new Put(rowkey.get());

            for (int i = 0; i < columns.length; i++) {
                HBaseUtil.addHBasePutColumn(put, cf, columns[i], arr[i]);
            }

            //context write
            context.write(rowkey, put);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        //get configuration
        Configuration conf = this.getConf();

        //create job
        Job job = Job.getInstance(conf, this.getClass().getSimpleName());

        //set run job class
        job.setJarByClass(CreateHttpHFile.class);

        //set job
        //step 1: set input
        Path inpath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inpath);

        //step 2: set map class
        job.setMapperClass(TransformHFileMapper.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        //step 3 : set reduce class
        job.setReducerClass(PutSortReducer.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(KeyValue.class);

        //step 4 : ouput
        Path outputDir = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputDir);

        //step 5 :
        // get table instance
        HTable table = new HTable(conf, args[0]);

        HFileOutputFormat2.configureIncrementalLoadMap(job, table);

        //submit job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("导入的表：" + args[0]);
        System.out.println("导入路径：" + args[1]);
        System.out.println("导出路径：" + args[2]);

        //get configuration
        Configuration conf = HBaseConfiguration.create();

        //run job
        int status = ToolRunner.run(conf, new CreateHttpHFile(), args);

        //exit program
        System.exit(status);

    }

}