package com.catchu.me;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 刘俊重
 * @Description 定义一个统计个数的Bolt
 * @date 16:40
 */
public class myCountBolt extends BaseRichBolt{

    OutputCollector collector;

    //最后统计的结果放在一个map中输出，key为单词，value为单词的个数
    Map<String,Integer> map = new HashMap<String,Integer>();

    /**
     * 初始化
     * @param map
     * @param topologyContext
     * @param collector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * storm会循环的调用这个方法，输入tuple，完成个数统计
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getString(0);
        Integer num = tuple.getInteger(1);
        if(map.containsKey(word)){
            map.put(word,map.get(word)+num);
        }else{
            map.put(word,num);
        }

        for(Map.Entry<String,Integer> m : map.entrySet()){
            System.out.println("单词："+m.getKey()+",个数："+m.getValue());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //不输出
    }
}
