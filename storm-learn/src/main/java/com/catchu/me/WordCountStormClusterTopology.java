package com.catchu.me;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WordCountStormClusterTopology {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout_id", new DataSourceSpout());
        builder.setBolt("spit_bolt_id", new SplitBolt()).shuffleGrouping("spout_id");
        builder.setBolt("wordcount_bolt_id", new WordCountBolt()).shuffleGrouping("spit_bolt_id");
        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
        config.setMaxTaskParallelism(1);
        localCluster.submitTopology(WordCountStormClusterTopology.class.getSimpleName() + System.currentTimeMillis(),
                config, builder.createTopology());
    }

    /**
     * 数据源
     *
     * @author shenfl
     *
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private static final long serialVersionUID = 1L;

        private Map conf;
        private TopologyContext context;
        private SpoutOutputCollector collector;

        /**
         * 初始化方法： 本实例运行的时候执行一次且仅一次
         */
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.conf = conf;
            this.context = context;
            this.collector = collector;
        }

        /**
         * 死循环调用： tuple 为storm传输数据基本单位
         */
        @Override
        public void nextTuple() {

            // 读取文件列表
            Collection<File> listFiles = FileUtils.listFiles(new File("d:/test"), new String[] { "txt" }, true);
            // 循环每个文件
            for (File file : listFiles) {
                // 行格式发送
                try {
                    List<String> lines = FileUtils.readLines(file);
                    for (String line : lines) {
                        this.collector.emit(new Values(line));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // 文件已经处理完成
                try {
                    File srcFile = file.getAbsoluteFile();
                    File destFile = new File(srcFile + ".done." + System.currentTimeMillis());
                    FileUtils.moveFile(srcFile, destFile);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * 单词分隔
     *
     * @author shenfl
     *
     */
    public static class SplitBolt extends BaseRichBolt {
        private Map stormConf;
        private TopologyContext context;
        private OutputCollector collector;
        /**
         *
         */
        private static final long serialVersionUID = 1L;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.stormConf = stormConf;
            this.context = context;
            this.collector = collector;
        }

        /**
         * 接收spout中的nextTuple，数据传输以Tuple为单位
         */
        @Override
        public void execute(Tuple input) {
            // 接收每行数据
            String line = input.getStringByField("line");
            String[] words = line.split("\t");
            for (String word : words) {
                // 每行数据分隔，单词为单位发射
                this.collector.emit(new Values(word));
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCountBolt extends BaseRichBolt {
        private Map stormConf;
        private TopologyContext context;
        private OutputCollector collector;
        /**
         * 单词出现的次数
         */
        public static Map<String, Integer> hashMap = new HashMap<String, Integer>();

        /**
         *
         */
        private static final long serialVersionUID = 1L;
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.stormConf = stormConf;
            this.context = context;
            this.collector = collector;
        }
        @Override
        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Integer value = hashMap.get(word);
            if (value == null) {
                hashMap.put(word, 0);
            } else {
                hashMap.put(word, value + 1);
            }

            for (Map.Entry<String, Integer> e : hashMap.entrySet()) {
                System.out.println("输出结果： =============="+e.getKey() + ":" + e.getValue());
            }
        }
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }
}

