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
 * Created by affo on 02/10/15.
 */
public class EmptyTaxisBolt extends WindowBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmptyTaxisBolt.class);
    private OutputCollector collector;

    public static final String FIELD_DATE_PICKUP_TS = "pickup_ts";
    public static final String FIELD_DATE_DROPOFF_TS = "dropoff_ts";
    public static final String FIELD_STRING_CELL = "cell";
    public static final String FIELD_STRING_EMPTY_TAXY_ID = "empty_taxi_id";

    /**
     * @param windowSize size of the window in seconds
     */
    public EmptyTaxisBolt(int windowSize) {
        super(windowSize);
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
    public void onWindow(List<Tuple> window) {
        if (window.isEmpty()) {
            LOG.info("Empty window passed. Doing nothing...");
            return;
        }

        Tuple trigger = window.get(window.size() - 1);
        Object puTs = trigger.getValueByField(DataGenerator.FIELD_DATE_PICKUP_TS);
        Object doTs = trigger.getValueByField(DataGenerator.FIELD_DATE_DROPOFF_TS);

        Map<String, String> emptyTaxis = getEmptyTaxis(window);
        for (Map.Entry<String, String> e : emptyTaxis.entrySet()) {
            this.collector.emit(
                    new Values(
                            puTs, doTs,
                            e.getValue(), // dropoff cell
                            e.getKey() // empty taxi ID
                    )
            );
        }
    }

    private Map<String, String> getEmptyTaxis(List<Tuple> window) {
        Map<String, String> res = new HashMap<>();

        for (Tuple t : window) {
            String taxiID = t.getStringByField(DataGenerator.FIELD_STRING_TAXI_ID);
            String cell = t.getStringByField(DataGenerator.FIELD_STRING_DROPOFF_CELL);

            res.put(taxiID, cell);
        }

        return res;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(
                        FIELD_DATE_PICKUP_TS,
                        FIELD_DATE_DROPOFF_TS,
                        FIELD_STRING_CELL,
                        FIELD_STRING_EMPTY_TAXY_ID
                )
        );
    }
}
