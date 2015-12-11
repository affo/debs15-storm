package storm;

import backtype.storm.tuple.Tuple;

/**
 * Created by affo on 12/3/15.
 */
public class SimpleDataWriter extends DataWriter {

    public SimpleDataWriter(String outputFileName) {
        super(outputFileName);
    }

    @Override
    protected String getNewLine(Tuple t) {
        String l = "{\n";

        for (int i = 0; i < RankingBolt.rankingLength; i++) {
            String cell = t.getStringByField(RankingBolt.FIELD_STRING_CELLS[i]);
            String profit = t.getValueByField(RankingBolt.FIELD_DOUBLE_PROFITABILITIES[i]).toString();
            l += "\t\'" + cell + "\': " + profit + ",\n";
        }

        l = l.substring(0, l.length() - 2); // remove last comma
        l += "\n}";
        return l;
    }
}
