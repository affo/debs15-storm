package trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.DataGenerator;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by affo on 13/11/15.
 */
public class Main {
    public static int PROFIT_WINDOW = 15 * 60 * 1000; // in ms
    public static int EMPTY_TAXIS_WINDOW = 30 * 60 * 1000; // in ms

    public static void main(String[] args) throws Exception {
        TridentTopology topology = new TridentTopology();

        Stream spout = topology.newStream("spout", new DataGenerator("/data.sample.csv"));

        TridentState profitW = spout
                .each(
                        new Fields(DataGenerator.FIELD_DATE_DROPOFF_TS),
                        new Windower.Timestamper(),
                        new Fields("long_ts")
                )
                .each(
                        new Fields(DataGenerator.FIELD_DOUBLE_FARE, DataGenerator.FIELD_DOUBLE_TIP),
                        new BaseFunction() {
                            @Override
                            public void execute(TridentTuple t, TridentCollector c) {
                                c.emit(new Values(t.getDouble(0) + t.getDouble(1)));
                            }
                        },
                        new Fields("fare+tip")
                )
                .partitionPersist(
                        new Windower.Factory<Double>(PROFIT_WINDOW),
                        new Fields(
                                DataGenerator.FIELD_STRING_DROPOFF_CELL,
                                "long_ts", "fare+tip"
                        ),
                        new ProfitUpdater(),
                        new Fields("ts", "cell", "profit", "median_out_of")
                );

        TridentState emptyTaxisW = spout
                .each(
                        new Fields(DataGenerator.FIELD_DATE_DROPOFF_TS),
                        new Windower.Timestamper(),
                        new Fields("long_ts")
                )
                .partitionPersist(
                        new Windower.Factory<String>(EMPTY_TAXIS_WINDOW),
                        new Fields(
                                DataGenerator.FIELD_STRING_TAXI_ID,
                                "long_ts",
                                DataGenerator.FIELD_STRING_DROPOFF_CELL
                        ),
                        new EmptyTaxisUpdater(),
                        new Fields("ts", "cell", "count", "count_out_of")
                );

        profitW.newValuesStream().each(new Fields("ts", "cell", "profit", "median_out_of"), new Print("PROFIT"));
        emptyTaxisW.newValuesStream().each(new Fields("ts", "cell", "count", "count_out_of"), new Print("EMPTY_TAXIS"));


        Config conf = new Config();
        //conf.setDebug(true);
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, topology.build());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, topology.build());

            Utils.sleep(10000);

            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}