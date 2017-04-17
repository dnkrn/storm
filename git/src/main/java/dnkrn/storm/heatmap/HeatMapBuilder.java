package dnkrn.storm.heatmap;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.model.LatLng;

import java.util.*;

/**
 * Created by dinakaran on 4/16/17.
 */
public class HeatMapBuilder extends BaseBasicBolt {

    private Map<Long, List<LatLng>> heatmaps;

    @Override
    public void prepare(Map config,
                        TopologyContext context) {
        heatmaps = new HashMap<Long, List<LatLng>>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (isTickTuple(tuple)) {
            emitHeatmap(basicOutputCollector);
        } else {
            Long time = tuple.getLongByField("time");
            LatLng geocode = (LatLng) tuple.getValueByField("geocode");
            Long timeInterval = selectTimeInterval(time);
            List<LatLng> checkins = getCheckinsForInterval(timeInterval);
            checkins.add(geocode);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private Long selectTimeInterval(Long time) {
        return time / (15 * 1000);
    }

    private List<LatLng> getCheckinsForInterval(Long timeInterval) {
        List<LatLng> hotzones = heatmaps.get(timeInterval);
        if (hotzones == null) {
            hotzones = new ArrayList<LatLng>();
            heatmaps.put(timeInterval, hotzones);
        }
        return hotzones;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 60);
        return conf;
    }

    private boolean isTickTuple(Tuple tuple) {
        String sourceComponent = tuple.getSourceComponent();
        String sourceStreamId = tuple.getSourceStreamId();
        return sourceComponent.equals(Constants.SYSTEM_COMPONENT_ID)
                && sourceStreamId.equals(Constants.SYSTEM_TICK_STREAM_ID);
    }


    private void emitHeatmap(BasicOutputCollector outputCollector) {
        Long now = System.currentTimeMillis();
        Long emitUpToTimeInterval = selectTimeInterval(now);
        Set<Long> timeIntervalsAvailable = heatmaps.keySet();
        for (Long timeInterval : timeIntervalsAvailable) {
            if (timeInterval <= emitUpToTimeInterval) {
                List<LatLng> hotzones = heatmaps.remove(timeInterval);
                outputCollector.emit(new Values(timeInterval, hotzones));
            }
        }
    }


}
