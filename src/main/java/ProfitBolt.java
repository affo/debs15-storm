import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by affo on 02/10/15.
 */
public class ProfitBolt extends WindowBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ProfitBolt.class);
    private OutputCollector collector;

    public static final String OUT_STREAM_ID = "profit_stream";

    public static final String FIELD_DATE_PICKUP_TS = "pickup_ts";
    public static final String FIELD_DATE_DROPOFF_TS = "dropoff_ts";
    public static final String FIELD_STRING_CELL = "cell";
    public static final String FIELD_DOUBLE_MEDIAN_PROFIT = "median_profit";

    /**
     * @param windowSize size of the window in seconds
     */
    public ProfitBolt(int windowSize) {
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

        Map<String, List<Double>> profitPerCell = getProfitPerCell(window);
        Map<String, Double> medianPerCell = getMedianPerCell(profitPerCell);
        for (Map.Entry<String, Double> e : medianPerCell.entrySet()) {
            this.collector.emit(
                    OUT_STREAM_ID,
                    new Values(
                            puTs, doTs,
                            e.getKey(), // cell
                            e.getValue() // median profit
                    )
            );
        }
    }

    private Map<String, List<Double>> getProfitPerCell(List<Tuple> window) {
        Map<String, List<Double>> profitPerCell = new HashMap<>();
        for (Tuple t : window) {
            String pickupCell = (String) t.getValueByField(DataGenerator.FIELD_STRING_PICKUP_CELL);
            double fare = (double) t.getValueByField(DataGenerator.FIELD_DOUBLE_FARE);
            double tip = (double) t.getValueByField(DataGenerator.FIELD_DOUBLE_TIP);

            List<Double> profit = profitPerCell.get(pickupCell);
            if (profit == null) {
                profit = new ArrayList<>();
            }

            profit.add(fare + tip);
            profitPerCell.put(pickupCell, profit);
        }
        return profitPerCell;
    }

    private Map<String, Double> getMedianPerCell(Map<String, List<Double>> profitPerCell) {
        Map<String, Double> medianPerCell = new HashMap<>();
        for (Map.Entry<String, List<Double>> e : profitPerCell.entrySet()) {
            medianPerCell.put(e.getKey(), median(e.getValue()));
        }
        return medianPerCell;
    }

    private Double median(List<Double> profits) {
        int size = profits.size();
        if (size % 2 == 0) {
            double first = profits.get((size / 2) - 1);
            double second = profits.get(size / 2);
            return (first + second) / 2;
        }

        return profits.get(size / 2);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(
                OUT_STREAM_ID,
                new Fields(
                        FIELD_DATE_PICKUP_TS,
                        FIELD_DATE_DROPOFF_TS,
                        FIELD_STRING_CELL,
                        FIELD_DOUBLE_MEDIAN_PROFIT
                )
        );
    }
}
