package storm;

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
public class ProfitabilityBolt extends WindowBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ProfitabilityBolt.class);
    private OutputCollector collector;

    public static final String FIELD_DATE_PICKUP_TS = "pickup_datetime";
    public static final String FIELD_DATE_DROPOFF_TS = "dropoff_datetime";
    public static final String FIELD_STRING_CELL = "cell";
    public static final String FIELD_DOUBLE_MEDIAN_PROFIT = "profit";
    public static final String FIELD_INTEGER_EMPTY_TAXIS = "no_empty_taxis";
    public static final String FIELD_DOUBLE_PROFITABILITY = "profitability";

    /**
     * @param windowSize size of the window in seconds
     */
    public ProfitabilityBolt(int windowSize) {
        super(windowSize);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        this.collector = outputCollector;
    }

    @Override
    public Date getTs(Tuple t) {
        String sid = t.getSourceStreamId();
        Date ts;

        switch (sid) {
            case EmptyTaxisCounterBolt.OUT_STREAM_ID: {
                ts = (Date) t.getValueByField(EmptyTaxisCounterBolt.FIELD_DATE_DROPOFF_TS);
                break;
            }
            case ProfitBolt.OUT_STREAM_ID: {
                ts = (Date) t.getValueByField(ProfitBolt.FIELD_DATE_DROPOFF_TS);
                break;
            }
            default:
                // Should never happen
                throw new RuntimeException("Got a tuple from unknown stream: " + sid);
        }

        return ts;
    }

    @Override
    public void onWindow(List<Tuple> window) {
        Map<String, Tuple> profits = getProfits(window);
        Map<String, Tuple> noEmptyTaxis = getNoEmptyTaxis(window);

        Tuple trigger = window.get(window.size() - 1);
        Object puTs = trigger.getValueByField(DataGenerator.FIELD_DATE_PICKUP_TS);
        Object doTs = trigger.getValueByField(DataGenerator.FIELD_DATE_DROPOFF_TS);

        for (Map.Entry<String, Tuple> e : profits.entrySet()) {
            Tuple numT = e.getValue();
            Tuple denT = noEmptyTaxis.get(e.getKey());

            if (denT == null) {
                // this means we have to wait for that tuple to come
                continue;
            }

            String cell = numT.getStringByField(ProfitBolt.FIELD_STRING_CELL);
            double num = numT.getDoubleByField(ProfitBolt.FIELD_DOUBLE_MEDIAN_PROFIT);
            int den = denT.getIntegerByField(EmptyTaxisCounterBolt.FIELD_INTEGER_NO_EMPTY_TAXIS);

            if (den == 0) {
                // no empty taxis
                // discard value as specified in FAQs
                continue;
            }

            this.collector.emit(
                    new Values(puTs, doTs, cell, num, den, num / den)
            );
        }
    }

    private Map<String, Tuple> getNoEmptyTaxis(List<Tuple> window) {
        Map<String, Tuple> res = new HashMap<>();

        for (Tuple t : window) {
            if (t.getSourceStreamId().equals(EmptyTaxisCounterBolt.OUT_STREAM_ID)) {
                long doTs = ((Date) t.getValueByField(EmptyTaxisCounterBolt.FIELD_DATE_DROPOFF_TS)).getTime();
                long puTs = ((Date) t.getValueByField(EmptyTaxisCounterBolt.FIELD_DATE_PICKUP_TS)).getTime();
                String cell = t.getStringByField(EmptyTaxisCounterBolt.FIELD_STRING_CELL);
                String key = doTs + puTs + cell;

                res.put(key, t);
            }
        }

        return res;
    }

    private Map<String, Tuple> getProfits(List<Tuple> window) {
        Map<String, Tuple> res = new HashMap<>();

        for (Tuple t : window) {
            if (t.getSourceStreamId().equals(ProfitBolt.OUT_STREAM_ID)) {
                long doTs = ((Date) t.getValueByField(ProfitBolt.FIELD_DATE_DROPOFF_TS)).getTime();
                long puTs = ((Date) t.getValueByField(ProfitBolt.FIELD_DATE_PICKUP_TS)).getTime();
                String cell = t.getStringByField(ProfitBolt.FIELD_STRING_CELL);
                String key = doTs + puTs + cell;

                res.put(key, t);
            }
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
                        FIELD_DOUBLE_MEDIAN_PROFIT,
                        FIELD_INTEGER_EMPTY_TAXIS,
                        FIELD_DOUBLE_PROFITABILITY
                )
        );
    }
}
