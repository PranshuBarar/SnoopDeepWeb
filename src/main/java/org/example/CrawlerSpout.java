package org.example;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CrawlerSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private List<String> urls;
    private static int index = 0;
    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;

        //Now here first of all we will try to fetch all the URLs from the seed_urls.json file
        //Here we have used Object Mapper and FileInputStream for reading the urls from the seed_urls.json file

        ObjectMapper mapper = new ObjectMapper();
        File file;
        FileInputStream fileInputStream = null;
        file = new File("src/main/resources/seed_urls.json");
        try {
            fileInputStream = new FileInputStream(file);
            urls = mapper.readValue(fileInputStream, new TypeReference<>() {
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (fileInputStream != null) {
                try {
                    fileInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //Now here we will continue emitting the urls until and unless count becomes equal to '4'
    @Override
    public void nextTuple() {
        if(index < 4){
            String url = urls.get(index);
            collector.emit(new Values(url));
            index++;
        }
    }

    //This function is for declaring fields for the output
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url"));
    }
}
