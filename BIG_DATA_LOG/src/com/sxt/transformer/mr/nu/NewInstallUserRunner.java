package com.sxt.transformer.mr.nu;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.sxt.common.DateEnum;
import com.sxt.common.EventLogConstants;
import com.sxt.common.EventLogConstants.EventEnum;
import com.sxt.common.GlobalConstants;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.dim.base.DateDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import com.sxt.transformer.model.value.reduce.MapWritableValue;
import com.sxt.transformer.mr.TransformerOutputFormat;
import com.sxt.util.JdbcManager;
import com.sxt.util.TimeUtil;

/**
 * 计算新增用户入口类
 * 
 * @author root
 *
 */
public class NewInstallUserRunner implements Tool {
    Configuration conf = null;

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\IDEA\\hadoop-common-2.2.0-bin-master");
        try {
            ToolRunner.run(new Configuration(),new NewInstallUserRunner(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"new member");
        job.setJarByClass(NewInstallUserRunner.class);
        this.processArgs(conf,strings);
        TableMapReduceUtil.initTableMapperJob(getScans(conf),NewInstallUserMapper.class,StatsUserDimension.class,TimeOutputValue.class,job,false);
        job.setReducerClass(NewInstallUserReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        //向mysql中输出类的类型
        job.setOutputFormatClass(TransformerOutputFormat.class);

        return job.waitForCompletion(true)?0:1;
    }
    private void processArgs(Configuration conf, String[] args){
        String date = null;
        for (int i = 0;i<args.length;i++){
            if("-d".equals(args[i])){
                if(i+1<args.length){
                    date = args[++i];
                }
            }
        }
        if(StringUtils.isNotBlank(date) || TimeUtil.isValidateRunningDate(date)){
            date = TimeUtil.getYesterday();
        }
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES,date);
    }
    /*
    从hbase中获取数据
    条件：
        1：时间范围
        2：事件类型()
        3：获取部分列
     */
    private List<Scan> getScans(Configuration conf){
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        Scan scan = new Scan();
        long time = TimeUtil.parseString2Long(date);
        String startRow = String.valueOf(time);
        String stopRow = String.valueOf(time+ GlobalConstants.DAY_OF_MILLISECONDS);
        scan.setStartRow(startRow.getBytes());
        scan.setStopRow(stopRow.getBytes());

        //给scan对象添加过滤器
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(EventLogConstants.EVENT_LOGS_FAMILY_NAME.getBytes(),EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME.getBytes(),CompareOp.EQUAL,EventEnum.LAUNCH.alias.getBytes());
        filters.addFilter(filter1);

        String[] columns = new String[]{EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,EventLogConstants.LOG_COLUMN_NAME_PLATFORM,EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,EventLogConstants.LOG_COLUMN_NAME_UUID};
        filters.addFilter(getFilter(columns));

        scan.setFilter(filters);
        //指定从那个hbase表中获取数据
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME,EventLogConstants.HBASE_NAME_EVENT_LOGS.getBytes());
        return Arrays.asList(scan);
    }

    private Filter getFilter(String[] columns) {
        int length = columns.length;
        byte[][] b = new byte[length][];
        for (int i=0; i<length;i++){
            b[i] = columns[i].getBytes();
        }
        return new MultipleColumnPrefixFilter(b);
    }

    @Override
    public void setConf(Configuration conf) {
        conf.set("hbase.zookeeper.quorum", "192.168.40.135:2181,192.168.40.134:2181,192.168.40.137:2181");
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("transformer-env.xml");
        this.conf = HBaseConfiguration.create(conf);

    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }
}
