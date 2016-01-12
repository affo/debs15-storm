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
public class ProfitabilityBolt extends BaseRichBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ProfitabilityBolt.class);
    private OutputCollector collector;
    private Map<Integer, Map<String, Double>> nums;
    private Map<Integer, Map<String, Integer>> dens;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.nums = new HashMap<>();
        this.dens = new HashMap<>();
    }

    @Override
    public void execute(Tuple t) {
        int windowID = t.getInteger(0);
        String cell = t.getString(1);
        Map<String, Double> n = nums.get(windowID);
        Map<String, Integer> d = dens.get(windowID);

        if (n == null) {
            n = new HashMap<>();
        }
        if (d == null) {
            d = new HashMap<>();
        }

        Double profit = n.get(cell);
        Integer count = d.get(cell);

        if (t.getSourceStreamId().equals(ProfitBolt.OUT_STREAM_ID)) {
            profit = t.getDouble(2);
            n.put(cell, profit);
        } else {
            count = t.getInteger(2);
            d.put(cell, count);
        }

        nums.put(windowID, n);
        dens.put(windowID, d);

        if (profit != null && count != null && count != 0) {
            collector.emit(new Values(windowID, cell, profit / count));
        }

        collector.ack(t);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("windowID", "cell", "profitability"));
    }
}
