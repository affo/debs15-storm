import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by affo on 02/10/15.
 */
public class EmptyTaxisBolt extends WindowBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmptyTaxisBolt.class);
    private OutputCollector collector;

    public static final String FIELD_DATE_PICKUP_TS = "pickup_ts";
    public static final String FIELD_DATE_DROPOFF_TS = "dropoff_ts";
    public static final String FIELD_STRING_CELL = "cell";
    public static final String FIELD_INTEGER_NO_EMPTY_TAXIS = "no_empty_taxis";

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
        Map<String, Set<String>> emptyTaxisPerCell = getEmptyTaxisPerCell(window);

        for (Map.Entry<String, Set<String>> e : emptyTaxisPerCell.entrySet()) {
            this.collector.emit(
                    new Values(
                            trigger.getValueByField(DataGenerator.FIELD_DATE_PICKUP_TS),
                            trigger.getValueByField(DataGenerator.FIELD_DATE_DROPOFF_TS),
                            e.getKey(), // cell
                            e.getValue().size() // number of empty taxis
                    )
            );
        }

    }

    private Map<String, Set<String>> getEmptyTaxisPerCell(List<Tuple> window) {
        Map<String, Set<String>> res = new HashMap<>();

        for (Tuple t : window) {
            String taxiID = t.getStringByField(DataGenerator.FIELD_STRING_TAXI_ID);
            String dropoffCell = t.getStringByField(DataGenerator.FIELD_STRING_DROPOFF_CELL);
            String pickupCell = t.getStringByField(DataGenerator.FIELD_STRING_PICKUP_CELL);

            Set<String> dropoff = res.get(dropoffCell);
            Set<String> pickup = res.get(pickupCell);

            if (dropoff == null) {
                dropoff = new HashSet<String>();
            }

            if (pickup == null) {
                pickup = new HashSet<String>();
            }

            dropoff.add(taxiID);
            pickup.remove(taxiID); // if pickup == dropoff, no problem

            res.put(dropoffCell, dropoff);
            res.put(pickupCell, pickup);
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
                        FIELD_INTEGER_NO_EMPTY_TAXIS
                )
        );
    }
}
