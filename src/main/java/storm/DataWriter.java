package storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.util.List;
import java.util.Map;

/**
 * Created by affo on 05/11/15.
 */
public class DataWriter extends BaseRichBolt {
    private String outputFileName;
    private BufferedWriter writer;
    private OutputCollector collector;

    public DataWriter(String outputFileName) {
        this.outputFileName = outputFileName;
    }

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

        this.collector = outputCollector;
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

        collector.ack(tuple);
    }

    protected String getNewLine(Tuple t) {
        List<Tuple> ranking = (List<Tuple>) t.getValue(0);
        String l = "{\n";

        for (Tuple r : ranking) {
            if (r != null) {
                String cell = r.getString(1);
                Double profit = r.getDouble(2);
                l += "\t\'" + cell + "\': " + profit + ",\n";
            } else {
                l += "\tNULL,\n";
            }
        }

        l = l.substring(0, l.length() - 2); // remove last comma
        l += "\n}";
        return l;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // no output field to declare
    }
}
