import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by affo on 02/10/15.
 */
public abstract class WindowBolt extends BaseRichBolt {
    private List<Tuple> window = new ArrayList<>();
    private int windowSize;

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
    }

    public abstract void onWindow(List<Tuple> window);

    public abstract Date getTs(Tuple t);
}
