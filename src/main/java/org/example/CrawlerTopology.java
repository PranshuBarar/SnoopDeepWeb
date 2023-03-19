package org.example;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class CrawlerTopology {
    public void crawlerTopologyInitializer() throws Exception {

        //We will initialize the Topology Builder
        TopologyBuilder builder = new TopologyBuilder();

        //We will set the spout
        builder.setSpout("CrawlerSpout", new CrawlerSpout());

        //We will set the bolt (and connect with the required spout)
        builder.setBolt("CrawlerBolt", new CrawlerBolt()).shuffleGrouping("CrawlerSpout");

        //We will initialize the config
        Config config = new Config();

        //Now we will create a localCluster and submit our topology to this cluster
        LocalCluster cluster =  new LocalCluster();
        try {
            cluster.submitTopology("CrawlerTopology", config, builder.createTopology());
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}