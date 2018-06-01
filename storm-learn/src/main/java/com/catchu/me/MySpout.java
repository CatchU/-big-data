package com.catchu.me;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * @author 刘俊重
 * @Description 自己写的spout,用于从外部数据源采集数据，转化成storm的数据，通过tuple分发给bolt
 * @date 15:45
 */
public class MySpout extends BaseRichSpout {

    SpoutOutputCollector collector;
    /**
     * 初始化方法
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    /**
     * storm框架会不停的调用这个nextTuple方法(死循环)，不停的给bolt发数据
     */
    @Override
    public void nextTuple() {
        String str = "i am lilei love hanmeimei";
        collector.emit(new Values(str));
    }

    /**
     * 定义一个输出的表头，tuple都emit（发射）到这个表头输出
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
