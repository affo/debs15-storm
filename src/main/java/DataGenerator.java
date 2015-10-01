import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by affo on 01/10/15.
 */
public class DataGenerator extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    public static final int NO_FIELDS = 17;
    private SpoutOutputCollector collector;
    private static final String dataPath = "data.sample.csv";
    private BufferedReader reader;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(
                        "medallion", //	an md5sum of the identifier of the taxi - vehicle bound
                        "hack_license", // an md5sum of the identifier for the taxi license
                        "pickup_datetime", // time when the passenger(s) were picked up
                        "dropoff_datetime", // time when the passenger(s) were dropped off
                        "trip_time_in_secs", // duration of the trip
                        "trip_distance", // trip distance in miles
                        "pickup_longitude", // longitude coordinate of the pickup location
                        "pickup_latitude", // latitude coordinate of the pickup location
                        "dropoff_longitude", // longitude coordinate of the drop-off location
                        "dropoff_latitude", // latitude coordinate of the drop-off location
                        "payment_type", // the payment method - credit card or cash
                        "fare_amount", // fare amount in dollars
                        "surcharge", // surcharge in dollars
                        "mta_tax", // tax in dollars
                        "tip_amount", // tip in dollars
                        "tolls_amount", // bridge and tunnel tolls in dollars
                        "total_amount" // total paid amount in dollars
                ));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.reader = new BufferedReader(
                new InputStreamReader(
                        getClass().getResourceAsStream(dataPath)
                )
        );
    }

    @Override
    public void nextTuple() {
        try {
            String line = this.reader.readLine();
            if (line != null) {
                String[] tokens = line.split(",");
                if (tokens.length != NO_FIELDS) {
                    throw new IOException("format error");
                }
                this.collector.emit(new Values((Object[]) tokens));
            }
            // else {
            // LOG.info(dataPath + ": FEOF");
            // }
        } catch (IOException e) {
            LOG.error("Error in reading nextTuple", e);
        }
    }
}
