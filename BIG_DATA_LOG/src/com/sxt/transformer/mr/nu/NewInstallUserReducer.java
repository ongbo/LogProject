package com.sxt.transformer.mr.nu;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.sxt.common.KpiType;
import com.sxt.transformer.model.dim.StatsUserDimension;
import com.sxt.transformer.model.value.map.TimeOutputValue;
import com.sxt.transformer.model.value.reduce.MapWritableValue;

/**
 * 计算new isntall user的reduce类
 * 输入： k:statsUserDimension
 *      v:tieOutputValue(uuid,time)
 * 输出： k:statsUserDimmension
 *       v:MapWritablVallue
 * @author root
 *
 */
public class NewInstallUserReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    //创建reduce输出的value对象
    MapWritableValue mapWritableValue = new MapWritableValue();
    //创建去重集合
    Set<String> unique = new HashSet<String>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
      //清空Set集合，防止上一个迭代器留下的值产生的影响
         this.unique.clear();
         //遍历迭代器，将set集合的大小作为最终统计结果
         for (TimeOutputValue timeOutputValue : values){
             this.unique.add(timeOutputValue.getId());
         }
        //存放最终的计算结果，key是唯一一个标识，方便取值，value是集合大小，最终的统计结果
         MapWritable map = new MapWritable();
         map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
         //将map结果放到reduce输出的value对象中
         mapWritableValue.setValue(map);
         //获取模块名称
        String kpiname = key.getStatsCommon().getKpi().getKpiName();
        //将kpiType设置奥reduce端输出对象中
        if(kpiname.equals(KpiType.NEW_INSTALL_USER.name)){
            mapWritableValue.setKpi(KpiType.NEW_INSTALL_USER);
        }else if(kpiname.equals(KpiType.BROWSER_NEW_INSTALL_USER.name)){
            mapWritableValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
        }
        context.write(key,mapWritableValue);
    }
}
