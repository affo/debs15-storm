package storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Map;

/**
 * Created by affo on 01/10/15.
 * <p/>
 * The tuple:
 * 0 - "medallion", //	an md5sum of the identifier of the taxi - vehicle bound
 * 1 - "hack_license", // an md5sum of the identifier for the taxi license
 * 2 - "pickup_datetime", // time when the passenger(s) were picked up
 * 3 - "dropoff_datetime", // time when the passenger(s) were dropped off
 * 4 - "trip_time_in_secs", // duration of the trip
 * 5 - "trip_distance", // trip distance in miles
 * 6 - "pickup_longitude", // longitude coordinate of the pickup location
 * 7 - "pickup_latitude", // latitude coordinate of the pickup location
 * 8 - "dropoff_longitude", // longitude coordinate of the drop-off location
 * 9 - "dropoff_latitude", // latitude coordinate of the drop-off location
 * 10 - "payment_type", // the payment method - credit card or cash
 * 11 - "fare_amount", // fare amount in dollars
 * 12 - "surcharge", // surcharge in dollars
 * 13 - "mta_tax", // tax in dollars
 * 14 - "tip_amount", // tip in dollars
 * 15 - "tolls_amount", // bridge and tunnel tolls in dollars
 * 16 - "total_amount" // total paid amount in dollars
 */
public class DataGenerator extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    public static final String PROFIT_STREAM_ID = "num";
    public static final String EMPTY_TAXIS_STREAM_ID = "den";
    boolean _feof;
    private SpoutOutputCollector collector;
    private String dataPath;
    private BufferedReader reader;

    private long lastTs;
    private int tupleID = 0;

    public DataGenerator(String dataPath) {
        this.dataPath = dataPath;
        this.lastTs = 0L;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        Fields fields =  new Fields("tupleID", "taxiRide", "pickupCell", "taxiID");
        outputFieldsDeclarer.declareStream(PROFIT_STREAM_ID, fields);
        outputFieldsDeclarer.declareStream(EMPTY_TAXIS_STREAM_ID, fields);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._feof = false;
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
                TaxiRide tr = TaxiRide.parse(line);
                Values tuple = new Values(tupleID, tr, tr.pickupCell, tr.taxiID);

                long newTs = tr.dropoffTS.getTime();
                if (lastTs > 0) {
                    // we have to sleep SECONDS_PER_TIME_UNIT
                    // for each TIME_UNIT_IN_SECONDS passed from last tuple
                    // to this one
                    long fromTupleToSystemTime = WindowBolt.TIME_UNIT_IN_SECONDS * WindowBolt.SECONDS_PER_TIME_UNIT;
                    long sleepTime = (newTs - lastTs) /  fromTupleToSystemTime;
                    Utils.sleep(sleepTime);
                }
                lastTs = newTs;

                this.collector.emit(PROFIT_STREAM_ID, tuple);
                this.collector.emit(EMPTY_TAXIS_STREAM_ID, tuple);
                tupleID++;
            } else if (!_feof) {
                LOG.info(dataPath + ": FEOF");
                _feof = true;
            }
        } catch (IOException e) {
            LOG.error("Error in reading nextTuple", e);
        } catch (AreaMapper.OutOfGridException e) {
            LOG.info(e.getMessage());
        } catch (ParseException e) {
            LOG.error("Error in parsing datetime", e);
        }
    }
}
