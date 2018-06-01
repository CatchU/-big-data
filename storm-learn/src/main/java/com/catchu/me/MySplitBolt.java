package com.catchu.me;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author 刘俊重
 * @Description 分割句子的bolt
 * @date 16:31
 */
public class MySplitBolt extends BaseRichBolt{

    //统计输出的tuple
    OutputCollector collector;

    /**
     * 初始化方法
     * @param map
     * @param topologyContext
     * @param outputCollector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    /**
     * 被storm框架循环调用，传入tuple
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        String str = tuple.getString(0);
        String[] split = str.split(" ");
        if(split!=null){
            for(String word : split){
                collector.emit(new Values(word,1));
            }
        }
    }

    /**
     * 定义一个输出的表头，tuple都射到这个表头里输出
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","num"));
    }
}
