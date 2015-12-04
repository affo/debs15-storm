package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/**
 * Created by affo on 01/10/15.
 */
public class Main {
    public static final int TOP_N = 10;
    public static int PROFIT_WINDOW = 15 * 60; // in seconds
    public static int EMPTY_TAXIS_WINDOW = 30 * 60; // in seconds
    public static int PROFITABILITY_WINDOW = 15 * 60; // in seconds

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("data", new DataGenerator("/data.sample.csv"), 1);

        builder.setBolt("profit", new ProfitBolt(PROFIT_WINDOW), 10)
                .fieldsGrouping(
                        "data",
                        DataGenerator.PROFIT_STREAM_ID,
                        new Fields(DataGenerator.FIELD_STRING_DROPOFF_CELL)
                );

        builder.setBolt("empty_taxis", new EmptyTaxisBolt(EMPTY_TAXIS_WINDOW), 10)
                .fieldsGrouping(
                        "data",
                        DataGenerator.EMPTY_TAXIS_STREAM_ID,
                        new Fields(DataGenerator.FIELD_STRING_TAXI_ID)
                );


        builder.setBolt("empty_taxis_counter", new EmptyTaxisCounterBolt(EMPTY_TAXIS_WINDOW), 10)
                .fieldsGrouping("empty_taxis", new Fields(EmptyTaxisBolt.FIELD_STRING_CELL));


        // joiner
        builder.setBolt("profitability", new ProfitabilityBolt(PROFITABILITY_WINDOW), 10)
                .fieldsGrouping(
                        "profit",
                        ProfitBolt.OUT_STREAM_ID,
                        new Fields(
                                ProfitBolt.FIELD_DATE_PICKUP_TS,
                                ProfitBolt.FIELD_DATE_DROPOFF_TS,
                                ProfitBolt.FIELD_STRING_CELL
                        )
                )
                .fieldsGrouping(
                        "empty_taxis_counter",
                        EmptyTaxisCounterBolt.OUT_STREAM_ID,
                        new Fields(
                                EmptyTaxisCounterBolt.FIELD_DATE_PICKUP_TS,
                                EmptyTaxisCounterBolt.FIELD_DATE_DROPOFF_TS,
                                EmptyTaxisCounterBolt.FIELD_STRING_CELL
                        )
                );

        builder.setBolt("rankings", new RankingBolt(TOP_N)).globalGrouping("profitability");

        builder.setBolt("to_file", new SimpleDataWriter()).globalGrouping("rankings");

        Config conf = new Config();

        // It seems that activating DEBUG causes
        // storm to never-ending output metrics... bah

        // conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
