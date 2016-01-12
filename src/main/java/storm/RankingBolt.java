package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by affo on 05/11/15.
 */
public class RankingBolt extends BaseRichBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RankingBolt.class);

    private OutputCollector collector;

    private Tuple[] ranking;
    public static int rankingLength;

    public RankingBolt(int topN) {
        rankingLength = topN;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.ranking = new Tuple[rankingLength];
    }

    @Override
    public void execute(Tuple t) {
        boolean changed = false;
        double currProfitability = t.getDouble(2);
        String currCell = t.getString(1);
        int windowID = t.getInteger(0);

        for (int i = 0; i < rankingLength && !changed; i++) {
            if (ranking[i] == null) {
                ranking[i] = t;
                changed = true;
            } else {
                double profitability = ranking[i].getDouble(2);
                String cell = ranking[i].getString(1);

                if (currProfitability > profitability) {
                    // shift right
                    for (int j = rankingLength - 1; j > i; j--) {
                        ranking[j] = ranking[j - 1];
                    }
                    ranking[i] = t;
                    changed = true;
                } else if (currCell.equals(cell) && currProfitability == profitability) {
                    // avoid that the same tuple received
                    // more times fills the rankings
                    break;
                }
            }
        }

        if (changed) {
            LOG.info(windowID + " -> " + currCell + ": " + currProfitability);
            this.collector.emit(new Values(Arrays.asList(this.ranking)));
        }

        this.collector.ack(t);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ranking"));
    }
}
