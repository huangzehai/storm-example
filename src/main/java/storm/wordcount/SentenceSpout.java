package storm.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by huangzehai on 2017/6/19.
 */
public class SentenceSpout extends BaseRichSpout {
    private ConcurrentHashMap<UUID, Values> pending;
    private SpoutOutputCollector collector;
    private String[] sentences = {
            "My dog has fleas",
            "I like cold beverage",
            "the dog adt my homework",
            "don't have a cow man",
            "I don't think I like fleas"};
    private int index = 0;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<>();
    }

    @Override
    public void nextTuple() {
        Values values = new Values(sentences[index]);
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values);
        this.collector.emit(values, msgId);
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.sleep(500);
    }

    @Override
    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    public void fail(Object msgId) {
        //重发失败的消息
        this.collector.emit(this.pending.get(msgId), msgId);
    }

}
