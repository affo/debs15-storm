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
 * Created by affo on 02/10/15.
 */
public class EmptyTaxisBolt extends WindowBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmptyTaxisBolt.class);
    private OutputCollector collector;

    /**
     * @param windowSize size of the window in seconds
     */
    public EmptyTaxisBolt(int windowSize) {
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
        Map<String, Tuple> emptyTaxis = getEmptyTaxis(window);
        for (Map.Entry<String, Tuple> e : emptyTaxis.entrySet()) {
            TaxiRide tr = ((TaxiRide) e.getValue().getValue(1));
            String taxiID = e.getKey();
            this.collector.emit(new Values(windowID, tr.dropoffCell, taxiID));
        }
    }

    private Map<String, Tuple> getEmptyTaxis(List<Tuple> window) {
        Map<String, Tuple> res = new HashMap<>();

        for (Tuple t : window) {
            String taxiID = ((TaxiRide) t.getValue(1)).taxiID;
            res.put(taxiID, t);
        }

        return res;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields("windowID", "cell", "taxiID")
        );
    }
}
