package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 02/10/15.
 */
public abstract class WindowBolt extends BaseRichBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(WindowBolt.class);
    private List<Tuple> window = new ArrayList<>();
    private int windowSize;
    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector  = outputCollector;
    }

    /**
     * @param windowSize size of the window in seconds
     */
    public WindowBolt(int windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public void execute(Tuple tuple) {
        long newTs = getTs(tuple).getTime();
        long windowSize = this.windowSize * 1000; // ms
        int removeStop = 0;
        for (Tuple t : window) {
            long ts = getTs(t).getTime();
            if (newTs - ts > windowSize) {
                removeStop++;
            } else {
                break;
            }
        }

        for (int i = 0; i < removeStop; i++) {
            window.remove(i);
        }

        window.add(tuple);
        onWindow(this.window);
        collector.ack(tuple);
    }

    public abstract void onWindow(List<Tuple> window);

    public abstract Date getTs(Tuple t);
}
