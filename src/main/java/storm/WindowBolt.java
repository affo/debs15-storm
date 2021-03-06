package storm;

import backtype.storm.Config;
import backtype.storm.Constants;
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
    // how often a tick tuple will be sent to our bolt
    public static final int SECONDS_PER_TIME_UNIT = 1;
    // mapping system time to time in tuples
    public static final int TIME_UNIT_IN_SECONDS = 60;

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(WindowBolt.class);
    private List<Tuple> window = new ArrayList<>();
    private int windowSize;
    private long lastTime;
    private OutputCollector collector;
    private int windowID;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector  = outputCollector;
        this.windowID = 0;
    }

    /**
     * @param windowSize size of the window in seconds
     */
    public WindowBolt(int windowSize) {
        this.windowSize = windowSize;
        this.lastTime = 0L;
    }

    @Override
    public void execute(Tuple tuple) {
        if (isTickTuple(tuple)) {
            advanceWindow();

            if (!window.isEmpty()) {
                // this is the TRIGGER (as in flink)
                // in this case we evaluate the onWindow
                // method every SECONDS_PER_TIME_UNIT which
                // is basically window granularity
                onWindow(windowID, window);
            }

            windowID++;
        } else {

            if (lastTime == 0) {
                // this is the first tuple received,
                // set lastTime to something
                lastTime = getTs(tuple).getTime();
            }

            if (!tupleInWindow(tuple)) {
                window.add(tuple);
            }
        }

        collector.ack(tuple);
    }

    private boolean tupleInWindow(Tuple tuple) {
        int id = tuple.getInteger(0);

        for (Tuple t : window) {
            int oid = t.getInteger(0);
            if (id == oid) {
                return true;
            }
        }

        return false;
    }

    private void advanceWindow() {
        if (lastTime == 0) {
            // Tick tuple received before first tuple.
            // Do nothing. We start advancing the window
            // from the first processed tuple on.
            return;
        }

        lastTime += TIME_UNIT_IN_SECONDS * 1000;
        long windowSize = this.windowSize * 1000; // ms
        int removeStop = 0;

        for (Tuple t : window) {
            long delta = lastTime - getTs(t).getTime();
            if (delta > windowSize) {
                removeStop++;
            } else {
                break;
            }
        }

        for (int i = 0; i < removeStop; i++) {
            window.remove(0);
        }
    }

    public abstract void onWindow(int windowID, List<Tuple> window);

    public abstract Date getTs(Tuple t);

    protected boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, SECONDS_PER_TIME_UNIT);
        return conf;
    }
}
