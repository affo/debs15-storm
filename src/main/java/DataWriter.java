import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.util.Map;

/**
 * Created by affo on 05/11/15.
 */
public class DataWriter extends BaseRichBolt {
    private static final String outputFileName = "rankings.output";
    private BufferedWriter writer;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            this.writer = new BufferedWriter(
                    new OutputStreamWriter(
                            new FileOutputStream(outputFileName)
                    )
            );
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in opening " + outputFileName);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String line = getNewLine(tuple);
        try {
            writer.write(line + "\n\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error in writing to " + outputFileName);
        }
    }

    private String getNewLine(Tuple t) {
        String l = "{\n\n";
        l += getKeyValue(t, RankingBolt.FIELD_DATE_PICKUP_TS);
        l += getKeyValue(t, RankingBolt.FIELD_DATE_DROPOFF_TS);
        l += "\n";

        for (int i = 0; i < RankingBolt.rankingLength; i++) {
            l += getKeyValue(t, RankingBolt.FIELD_STRING_CELLS[i]);
            l += getKeyValue(t, RankingBolt.FIELD_INTEGER_NO_EMPTY_TAXIS[i]);
            l += getKeyValue(t, RankingBolt.FIELD_DOUBLE_MEDIAN_PROFITS[i]);
            l += getKeyValue(t, RankingBolt.FIELD_DOUBLE_PROFITABILITIES[i]);
            l += "\n";
        }

        l += "}";
        return l;
    }

    private String getKeyValue(Tuple t, String field) {
        return "\t" + field + ": " + t.getValueByField(field).toString() + ",\n";
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output field to declare
    }
}
