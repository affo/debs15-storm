package trident;

import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by affo on 13/11/15.
 */
public class Print extends BaseFilter {
    private org.slf4j.Logger LOG;
    private String logName;

    public Print(String logName) {
        this.logName = logName;
        this.LOG = LoggerFactory.getLogger(this.logName);
    }

    @Override
    public boolean isKeep(TridentTuple t) {
        String logstr = "";
        for (Object o : t.getValues()) {
            logstr += o.toString() + ", ";
        }
        LOG.info(logstr);
        return true;
    }
}
