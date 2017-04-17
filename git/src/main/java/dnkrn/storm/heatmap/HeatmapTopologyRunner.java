package dnkrn.storm.heatmap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * Created by dinakaran on 4/16/17.
 */
public class HeatmapTopologyRunner {

    public StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("checkins", new Checkins(),5);
        builder.setBolt("geocode-lookup", new GeocodeLookup(),5)
                .shuffleGrouping("checkins",);
        builder.setBolt("heatmap-builder", new HeatMapBuilder(),5)
                .globalGrouping("geocode-lookup");

       // builder.setBolt("persistor", new Persistor())
         //       .shuffleGrouping("heatmap-builder");


        return builder.createTopology();
    }

    public static void main(String[] args) {
        Config config = new Config();
        StormTopology topology = new HeatmapTopologyRunner().build();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("local-heatmap", config, topology);

    }
}
