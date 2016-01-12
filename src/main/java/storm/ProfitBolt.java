package storm;

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

    /**
     * @param windowSize size of the window in seconds
     */
    public ProfitBolt(int windowSize) {
        super(windowSize);
    }

    @Override
    public Date getTs(Tuple t) {
        return ((TaxiRide) t.getValue(1)).dropoffTS;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        super.prepare(map, topologyContext, outputCollector);
        this.collector = outputCollector;
    }

    @Override
    public void onWindow(int windowID, List<Tuple> window) {
        Map<String, List<Double>> profitPerCell = getProfitPerCell(window);
        Map<String, Double> medianPerCell = getMedianPerCell(profitPerCell);
        for (Map.Entry<String, Double> e : medianPerCell.entrySet()) {
            this.collector.emit(
                    OUT_STREAM_ID,
                    new Values(
                            windowID,
                            e.getKey(), // cell
                            e.getValue() // median profit
                    )
            );
        }
    }

    private Map<String, List<Double>> getProfitPerCell(List<Tuple> window) {
        Map<String, List<Double>> profitPerCell = new HashMap<>();
        for (Tuple t : window) {
            TaxiRide tr = ((TaxiRide) t.getValue(1));
            String pickupCell = tr.pickupCell;
            double fare = tr.fare;
            double tip = tr.tip;

            List<Double> profit = profitPerCell.get(pickupCell);
            if (profit == null) {
                profit = new ArrayList<>();
            }

            int i = 0;
            for ( ; i < profit.size(); i++) {
                if(profit.get(i) > fare + tip) {
                    break;
                }
            }

            profit.add(i, fare + tip);


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
                new Fields("windowID", "cell", "profit")
        );
    }
}
