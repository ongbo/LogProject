package com.sxt.transformer.mr.nu;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.sxt.common.DateEnum;
import com.sxt.common.EventLogConstants;
import com.sxt.common.KpiType;
import com.sxt.transformer.model.dim.StatsCommonDimension;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.dim.base.BrowserDimension;
import com.sxt.transformer.model.dim.base.DateDimension;
import com.sxt.transformer.model.dim.base.KpiDimension;
import com.sxt.transformer.model.dim.base.PlatformDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;

/**
 * 自定义的计算新用户的mapper类
 * 
 * @author root
 *
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension,TimeOutputValue>{//每个分析条件（由各个维度组成的）作为key，uuid作为value

    //定义列族
    byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    //定义输出map端输出key对象，和value对象
    StatsUserDimension statsUserDimension = new StatsUserDimension();
    TimeOutputValue timeOutputValue = new TimeOutputValue();

    //定义模块维度
    //暂时定义一个用户进本信息模块
    KpiDimension newInstallUser = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
    //浏览器模块
    KpiDimension InstallUserBrowser = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //从hbase中获取时间的值.
        String date = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME))));
        //获取平台维度的值
        String platform = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM))));
        //获取用户id
        String uuid = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID))));
        //浏览器——————名称和版本
        String browserName = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME))));
        String browserVersion = Bytes.toString(CellUtil.cloneValue(value.getColumnLatestCell(family,Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION))));

        //构建时间维度
        long time = Long.valueOf(date);
        DateDimension dateDimension = DateDimension.buildDate(time,DateEnum.DAY);
        //构建平台维度
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);
        //构建浏览器维度（名字和版本）
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName,browserVersion);



        //给输出的value对象timeOutputValue
        timeOutputValue.setId(uuid);
        timeOutputValue.setTime(time);

        /*
           *  拼接维度组合
         */

        //拼接时间维度
        //statsUserDimension.getStatsCommon().setDate(dateDimension);
        StatsCommonDimension statsCommonDimension = statsUserDimension.getStatsCommon();
        statsCommonDimension.setDate(dateDimension);

        //对平台和浏览器维度进行拼接，是一个嵌套循环循环，用循环是因为平台分为all和具体的平台，浏览器版本分为all和具体的浏览器版本。
        //设置空的浏览器
        BrowserDimension defaultBrowser = new BrowserDimension("","");
        for (PlatformDimension pl:platformDimensions){
            statsCommonDimension.setKpi(newInstallUser);//设置用户模块
            statsCommonDimension.setPlatform(pl);//设置平台
            statsUserDimension.setBrowser(defaultBrowser);//用户模块里面没有浏览器信息，所以设置一个空的就行
            context.write(statsUserDimension,timeOutputValue);
            for (BrowserDimension bd : browserDimensions){
                statsCommonDimension.setKpi(InstallUserBrowser);//设置浏览器模块
                statsUserDimension.setBrowser(bd);//设置浏览器信息
                context.write(statsUserDimension,timeOutputValue);
            }
        }
    }
}
