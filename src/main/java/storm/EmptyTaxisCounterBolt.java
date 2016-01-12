package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by affo on 03/11/15.
 */
public class EmptyTaxisCounterBolt extends BaseRichBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmptyTaxisCounterBolt.class);
    private OutputCollector collector;
    private Map<String, Integer[]> counter;

    public static final String OUT_STREAM_ID = "no_empty_taxis_stream";

    @Override
    public void execute(Tuple t) {
        int windowID = t.getInteger(0);
        String cell = t.getString(1);

        Integer[] count = counter.get(cell);

        if (count == null) {
            count = new Integer[2];
            count[0] = windowID;
            count[1] = 0;
        }

        if (windowID > count[0]) {
            this.collector.emit(
                    OUT_STREAM_ID,
                    new Values(count[0], cell, count[1])
            );
            counter.remove(cell);
        } else if (windowID == count[0]) {
            count[1]++;
            counter.put(cell, count);
        } else {
            // should never happen
            throw new RuntimeException("Out of sequence window ID: " + windowID + " < " + count[0]);
        }
    }


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counter = new HashMap<>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(
                OUT_STREAM_ID,
                new Fields("windowID", "cell", "empty_taxis_count")
        );
    }
}
