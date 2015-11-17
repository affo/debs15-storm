package trident;

import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 17/11/15.
 */
public class ProfitUpdater extends Windower.Updater<Double> {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ProfitUpdater.class);

    @Override
    protected void onUpdate(Windower<Double> windower, Long newTs, TridentCollector coll) {
        for (String cell : windower.ids()) {
            List<Double> sorted = new ArrayList<>();
            for (Map.Entry<Long, Double> e : windower.getWindow(cell)) {
                int i = 0;
                while (i < sorted.size() && sorted.get(i) < e.getValue()) {
                    i++;
                }
                sorted.add(i, e.getValue());
            }


            if (!sorted.isEmpty()) {
                coll.emit(new Values(newTs, cell, median(sorted), sorted.size()));
            }
        }
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
}
