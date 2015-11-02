import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 03/11/15.
 */
public class EmptyTaxisCounterBolt extends WindowBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmptyTaxisCounterBolt.class);
    private OutputCollector collector;

    public static final String FIELD_DATE_PICKUP_TS = "pickup_ts";
    public static final String FIELD_DATE_DROPOFF_TS = "dropoff_ts";
    public static final String FIELD_STRING_CELL = "cell";
    private static final String FIELD_INTEGER_NO_EMPTY_TAXIS = "no_empty_taxis";

    /**
     * @param windowSize size of the window in seconds
     */
    public EmptyTaxisCounterBolt(int windowSize) {
        super(windowSize);
    }

    @Override
    public void onWindow(List<Tuple> window) {
        if (window.isEmpty()) {
            LOG.info("Empty window passed. Doing nothing...");
            return;
        }

        Map<String, Integer> counter = new HashMap<>();

        for (Tuple t : window) {
            String cell = t.getStringByField(EmptyTaxisBolt.FIELD_STRING_CELL);
            Integer count = counter.get(cell);
            if (count == null) {
                count = 0;
            }
            count++;
            counter.put(cell, count);
        }

        Tuple trigger = window.get(window.size() - 1);

        for (Map.Entry<String, Integer> e : counter.entrySet()) {
            this.collector.emit(
                    new Values(
                            trigger.getValueByField(DataGenerator.FIELD_DATE_PICKUP_TS),
                            trigger.getValueByField(DataGenerator.FIELD_DATE_DROPOFF_TS),
                            e.getKey(), // cell
                            e.getValue() // # empty taxis
                    )
            );
        }
    }

    @Override
    public Date getTs(Tuple t) {
        return (Date) t.getValueByField(DataGenerator.FIELD_DATE_DROPOFF_TS);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(
                        FIELD_DATE_PICKUP_TS,
                        FIELD_DATE_DROPOFF_TS,
                        FIELD_STRING_CELL,
                        FIELD_INTEGER_NO_EMPTY_TAXIS
                )
        );
    }
}
