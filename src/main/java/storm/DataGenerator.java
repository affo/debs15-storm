package storm;

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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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
    private static final int NO_FIELDS = 17;
    private SpoutOutputCollector collector;
    private static final String dataPath = "/data.sample.csv";
    private BufferedReader reader;
    private DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); // 2013-01-01 00:02:00

    public static final String FIELD_STRING_TAXI_ID = "taxi_id";
    public static final String FIELD_STRING_LICENSE = "license";
    public static final String FIELD_DATE_PICKUP_TS = "pickup_ts";
    public static final String FIELD_DATE_DROPOFF_TS = "dropoff_ts";
    public static final String FIELD_STRING_PICKUP_CELL = "pickup_cell";
    public static final String FIELD_STRING_DROPOFF_CELL = "dropoff_cell";
    public static final String FIELD_DOUBLE_FARE = "fare";
    public static final String FIELD_DOUBLE_TIP = "tip";

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(
                new Fields(
                        FIELD_STRING_TAXI_ID,
                        FIELD_STRING_LICENSE,
                        FIELD_DATE_PICKUP_TS,
                        FIELD_DATE_DROPOFF_TS,
                        FIELD_STRING_PICKUP_CELL,
                        FIELD_STRING_DROPOFF_CELL,
                        FIELD_DOUBLE_FARE,
                        FIELD_DOUBLE_TIP
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

                double puLat = Double.valueOf(tokens[7]);
                double puLong = Double.valueOf(tokens[6]);
                double doLat = Double.valueOf(tokens[9]);
                double doLong = Double.valueOf(tokens[8]);
                String pickupCell = AreaMapper.getCellID(puLat, puLong);
                String dropoffCell = AreaMapper.getCellID(doLat, doLong);

                String taxiID = tokens[0];
                String license = tokens[1];
                Date pickupTs = dateFmt.parse(tokens[2]);
                Date dropoffTs = dateFmt.parse(tokens[3]);
                double fare = Double.valueOf(tokens[11]);
                double tip = Double.valueOf(tokens[14]);

                this.collector.emit(
                        new Values(
                                taxiID,
                                license,
                                pickupTs,
                                dropoffTs,
                                pickupCell,
                                dropoffCell,
                                fare,
                                tip
                        )
                );
            }
            // else {
            // LOG.info(dataPath + ": FEOF");
            // }
        } catch (IOException e) {
            LOG.error("Error in reading nextTuple", e);
        } catch (AreaMapper.OutOfGridException e) {
            LOG.info(e.getMessage());
        } catch (ParseException e) {
            LOG.error("Error in parsing datetime", e);
        }
    }
}
