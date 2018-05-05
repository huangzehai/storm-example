package storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import java.util.concurrent.TimeUnit;

/**
 * Created by lenovo on 2017/6/19.
 */
public class WordCountTopology {
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_BOLT_ID = "split-bolt";
    private static final String COUNT_BOLD_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";
    private static final String SLIDING_WINDOW_BOLT_ID = "sliding-window-bolt";
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {
        SentenceSpout spout = new SentenceSpout();
        SplitSentenceBolt splitBolt = new SplitSentenceBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();
        BaseWindowedBolt slidingWindowBolt = new SlidingWindowBolt().withWindow(new BaseWindowedBolt.Duration(6, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS));

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SENTENCE_SPOUT_ID, spout, 2);
        topologyBuilder.setBolt(SPLIT_BOLT_ID, splitBolt, 2).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);
        topologyBuilder.setBolt(COUNT_BOLD_ID, wordCountBolt, 4).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
        topologyBuilder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLD_ID);
        topologyBuilder.setBolt(SLIDING_WINDOW_BOLT_ID, slidingWindowBolt).globalGrouping(SPLIT_BOLT_ID);

        Config config = new Config();
        config.setNumWorkers(2);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());
            Utils.sleep(20000);
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
        }

    }
}
