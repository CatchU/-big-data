package com.catchu.me;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author 刘俊重
 * @Description 单词计数拓扑主程序
 * @date 16:47
 */
public class WordCountTopologyMain {

    public static void main(String[] args) {
        //1.创建一个topologybuilder，
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("mySpout",new MySpout(),2);
        topologyBuilder.setBolt("mySplitBolt",new MySplitBolt(),2).shuffleGrouping("mySpout");
        topologyBuilder.setBolt("myCountBolt",new myCountBolt(),4).
                fieldsGrouping("mySplitBolt",new Fields("word"));
        //2.创建一个configuration，指定需要的worker数
        Config config = new Config();
        config.setNumWorkers(2);
        //3.提交任务，分为本地提交和集群提交
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("myCount",config,topologyBuilder.createTopology());
    }
}
