package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 05/11/15.
 */
public class RankingBolt extends BaseRichBolt {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RankingBolt.class);

    /*
    The result stream of the query must provide the 10 most profitable areas in the subsequent format:
        pickup_datetime, dropoff_datetime,
        profitable_cell_id_1, empty_taxies_in_cell_id_1, median_profit_in_cell_id_1, profitability_of_cell_1,
        ... ,
        profitable_cell_id_10, empty_taxies_in_cell_id_10, median_profit_in_cell_id_10, profitability_of_cell_10,
        delay
     */
    public static final String FIELD_DATE_PICKUP_TS = "pickup_datetime";
    public static final String FIELD_DATE_DROPOFF_TS = "dropoff_datetime";
    public static String[] FIELD_STRING_CELLS;
    public static String[] FIELD_INTEGER_NO_EMPTY_TAXIS;
    public static String[] FIELD_DOUBLE_MEDIAN_PROFITS;
    public static String[] FIELD_DOUBLE_PROFITABILITIES;

    private OutputCollector collector;

    private Tuple[] ranking;
    public static int rankingLength;

    public RankingBolt(int topN) {
        rankingLength = topN;
        FIELD_STRING_CELLS = new String[rankingLength];
        for (int i = 0; i < rankingLength; i++) {
            FIELD_STRING_CELLS[i] = "profitable_cell_id_" + (i + 1);
        }

        FIELD_INTEGER_NO_EMPTY_TAXIS = new String[rankingLength];
        for (int i = 0; i < rankingLength; i++) {
            FIELD_INTEGER_NO_EMPTY_TAXIS[i] = "empty_taxies_in_cell_id_" + (i + 1);
        }

        FIELD_DOUBLE_MEDIAN_PROFITS = new String[rankingLength];
        for (int i = 0; i < rankingLength; i++) {
            FIELD_DOUBLE_MEDIAN_PROFITS[i] = "median_profit_in_cell_id_" + (i + 1);
        }

        FIELD_DOUBLE_PROFITABILITIES = new String[rankingLength];
        for (int i = 0; i < rankingLength; i++) {
            FIELD_DOUBLE_PROFITABILITIES[i] = "profitability_of_cell_" + (i + 1);
        }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.ranking = new Tuple[rankingLength];
    }

    @Override
    public void execute(Tuple t) {
        boolean changed = false;
        double currProfitability = t.getDoubleByField(ProfitabilityBolt.FIELD_DOUBLE_PROFITABILITY);
        String currCell = t.getStringByField(ProfitabilityBolt.FIELD_STRING_CELL);

        for (int i = 0; i < rankingLength && !changed; i++) {
            if (ranking[i] == null) {
                ranking[i] = t;
                changed = true;
            } else {
                double profitability = ranking[i].getDoubleByField(ProfitabilityBolt.FIELD_DOUBLE_PROFITABILITY);
                String cell = ranking[i].getStringByField(ProfitabilityBolt.FIELD_STRING_CELL);

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
            emitRanking(t);
        }
    }

    private void emitRanking(Tuple t) {
        Object[] vals = new Object[rankingLength * 4 + 2];
        Date doTs = (Date) t.getValueByField(ProfitabilityBolt.FIELD_DATE_DROPOFF_TS);
        Date puTs = (Date) t.getValueByField(ProfitabilityBolt.FIELD_DATE_PICKUP_TS);
        vals[0] = puTs;
        vals[1] = doTs; // + 2

        for (int i = 0; i < rankingLength; i++) {
            int valsI = (i * 4) + 2;
            if (ranking[i] == null) {
                vals[valsI] = "NULL";
                vals[valsI + 1] = "NULL";
                vals[valsI + 2] = "NULL";
                vals[valsI + 3] = "NULL";
            } else {
                vals[valsI] = ranking[i].getValueByField(ProfitabilityBolt.FIELD_STRING_CELL);
                vals[valsI + 1] = ranking[i].getValueByField(ProfitabilityBolt.FIELD_INTEGER_EMPTY_TAXIS);
                vals[valsI + 2] = ranking[i].getValueByField(ProfitabilityBolt.FIELD_DOUBLE_MEDIAN_PROFIT);
                vals[valsI + 3] = ranking[i].getValueByField(ProfitabilityBolt.FIELD_DOUBLE_PROFITABILITY);
            }
        }

        this.collector.emit(new Values(vals));
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        List<String> fields = new ArrayList<>();
        fields.add(FIELD_DATE_PICKUP_TS);
        fields.add(FIELD_DATE_DROPOFF_TS);

        for (int i = 0; i < rankingLength; i++) {
            fields.add(FIELD_STRING_CELLS[i]);
            fields.add(FIELD_INTEGER_NO_EMPTY_TAXIS[i]);
            fields.add(FIELD_DOUBLE_MEDIAN_PROFITS[i]);
            fields.add(FIELD_DOUBLE_PROFITABILITIES[i]);
        }

        outputFieldsDeclarer.declare(new Fields(fields));
    }
}
