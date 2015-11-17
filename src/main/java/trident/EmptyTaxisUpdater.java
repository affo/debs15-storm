package trident;

import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 17/11/15.
 */
public class EmptyTaxisUpdater extends Windower.Updater<String> {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmptyTaxisUpdater.class);

    @Override
    protected void onUpdate(Windower<String> windower, Long newTs, TridentCollector coll) {
        Map<String, Integer> counter = new HashMap<>();
        for (String taxiID : windower.ids()) {
            List<Map.Entry<Long, String>> locations = windower.getWindow(taxiID);
            if (!locations.isEmpty()) {
                String lastCell = locations.get(locations.size() - 1).getValue(); // last dropoff cell

                Integer count = counter.get(lastCell);
                if (count == null) {
                    count = 0;
                }
                count++;
                counter.put(lastCell, count);
            }
        }


        for (Map.Entry<String, Integer> count : counter.entrySet()) {
            coll.emit(new Values(newTs, count.getKey(), count.getValue(), counter.size()));
        }
    }
}
