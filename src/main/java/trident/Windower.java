package trident;

import backtype.storm.task.IMetricsContext;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.tuple.TridentTuple;

import java.util.*;

/**
 * Created by affo on 16/11/15.
 * <p/>
 * Pass in tuples with a String ID at position 0 (could be the same for every tuple...),
 * a Long Timestamp at position 1, and the value to store (type T) at position 2.
 * <p/>
 * In case you have a java.util.Date as Timestamp
 * use Windower.Timestamper function to convert it to a Long.
 */
public class Windower<T> implements State {
    private HashMap<String, List<Map.Entry<Long, T>>> window;
    private long windowSize;

    public static class Factory<T> implements StateFactory {
        private long windowSize;

        /**
         * @param windowSize in milliseconds
         */
        public Factory(long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public State makeState(Map conf, IMetricsContext iMetricsContext, int i, int i1) {
            return new Windower<T>(windowSize);
        }
    }

    private Windower(long windowSize) {
        this.windowSize = windowSize;
        this.window = new HashMap<>();
    }

    public abstract static class Updater<T> extends BaseStateUpdater<Windower<T>> {

        @Override
        public void updateState(Windower<T> windower, List<TridentTuple> list, TridentCollector coll) {
            TridentTuple last = list.get(list.size() - 1);
            long newTs = last.getLong(1);
            windower.bulkAdd(list, newTs);
            this.onUpdate(windower, newTs, coll);
        }

        // emitted values will be published
        // to newValuesStream()
        /*
            for (TridentTuple t : windower.getWindow()) {
                coll.emit(t.getValues());
            }
        */
        protected abstract void onUpdate(Windower<T> windower, Long newTs, TridentCollector coll);
    }

    public static class Timestamper extends BaseFunction {

        @Override
        public void execute(TridentTuple t, TridentCollector coll) {
            Date ts = (Date) t.getValue(0);
            coll.emit(new Values(ts.getTime()));
        }
    }

    @Override
    public void beginCommit(Long aLong) {
    }

    @Override
    public void commit(Long aLong) {
    }

    public void bulkAdd(List<TridentTuple> tuples, Long newTs) {
        // update every window
        for (String id : ids()) {
            int removeStop = 0;
            List<Map.Entry<Long, T>> l = getWindow(id);
            if (l == null) {
                l = new ArrayList<>();
            }

            for (Map.Entry<Long, T> e : l) {
                long oldTs = e.getKey();
                if (newTs - oldTs > windowSize) {
                    removeStop++;
                } else {
                    break;
                }
            }

            for (int i = 0; i < removeStop; i++) {
                l.remove(0);
            }
            window.put(id, l);
        }

        for (final TridentTuple t : tuples) {
            String key = t.getString(0);
            List<Map.Entry<Long, T>> l = window.get(key);
            if (l == null) {
                l = new ArrayList<>();
            }
            l.add(new Map.Entry<Long, T>() {
                @Override
                public Long getKey() {
                    return t.getLong(1);
                }

                @Override
                public T getValue() {
                    return (T) t.getValue(2);
                }

                @Override
                public T setValue(T t) {
                    // does nothing...
                    // our entries are immutable
                    return null;
                }
            });
            window.put(key, l);
        }
    }

    public Set<String> ids() {
        return this.window.keySet();
    }

    public List<Map.Entry<Long, T>> getWindow(String id) {
        return window.get(id);
    }
}
