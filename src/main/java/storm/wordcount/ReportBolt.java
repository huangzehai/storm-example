package storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by lenovo on 2017/6/19.
 */
public class ReportBolt extends BaseRichBolt {
    private HashMap<String, Long> counts = null;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        counts = new HashMap<>();
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
//        System.out.println("report: " + word + ":" + count);
        //新的计数会覆盖旧的计数
        counts.put(word, count);
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // this bolt does not emit anything
    }

    @Override
    public void cleanup() {
        System.out.println("-------Final Counts-------");
        List<String> keys = new ArrayList<>();
        keys.addAll(counts.keySet());
        Collections.sort(keys);
        for (String key : keys) {
            System.out.println(key + ":" + counts.get(key));
        }
        System.out.println("--------------------------");
    }
}
