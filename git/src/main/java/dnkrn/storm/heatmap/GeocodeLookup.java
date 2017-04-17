package dnkrn.storm.heatmap;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.*;

import java.io.IOException;
import java.util.Map;

/**
 * Created by dinakaran on 4/16/17.
 */
public class GeocodeLookup extends BaseBasicBolt {

    private Geocoder geocoder;
    @Override
    public void prepare(Map config,
                        TopologyContext context) {
        geocoder = new Geocoder();
    }
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String address = tuple.getStringByField("address");
        Long time = tuple.getLongByField("time");
        GeocoderRequest request = new GeocoderRequestBuilder()
                .setAddress(address)
                .setLanguage("en")
                .getGeocoderRequest();
        GeocodeResponse response = null;
        try {
            response = geocoder.geocode(request);
        } catch (IOException e) {
            e.printStackTrace();
        }
        GeocoderStatus status = response.getStatus();
        if (GeocoderStatus.OK.equals(status)) {
            GeocoderResult firstResult = response.getResults().get(0);
            LatLng latLng = firstResult.getGeometry().getLocation();
            basicOutputCollector.emit(new Values(time, latLng));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time", "geocode"));
    }
}
