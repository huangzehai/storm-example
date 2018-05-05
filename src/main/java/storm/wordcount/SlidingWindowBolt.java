package storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

public class SlidingWindowBolt extends BaseWindowedBolt {

    private OutputCollector collector;
    private Map<String, Long> counts;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        counts = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        System.out.println("+----------one window data----------+");
        counts.clear();
        for (Tuple tuple : tupleWindow.get()) {
            String word = tuple.getStringByField("word");
            Long count = counts.get(word);
            if (count == null) {
                count = new Long(0);
            }
            count++;
            counts.put(word, count);
            this.collector.ack(tuple);
        }

        for (String word : counts.keySet()) {
            System.out.println(word + ":" + counts.get(word));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
