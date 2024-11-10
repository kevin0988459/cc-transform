package com.cloudcomputing.samza.nycabs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Consumes the stream of ad-click. Outputs a stream which handles static file
 * and one stream and gives a stream of revenue distribution.
 */
public class AdPriceTask implements StreamTask, InitableTask {

    /*
       Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
     */
    private KeyValueStore<String, Integer> adPriceStore;
    private Map<String, Integer> adPrices;
    private ObjectMapper objectMapper;
    private SystemStream adPriceStream;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        // Initialize (maybe kv store and static data?)
        adPriceStore = (KeyValueStore<String, Integer>) context.getTaskContext().getStore("ad-price-store");
        adPrices = new HashMap<>();
        objectMapper = new ObjectMapper();

        // Load ad prices
        List<String> adPricesRawString = AdPriceConfig.readFile("NYCstoreAds.json");
        for (String rawString : adPricesRawString) {
            Map<String, Object> mapResult = objectMapper.readValue(rawString, HashMap.class);
            String storeId = (String) mapResult.get("storeId");
            int adPrice = (Integer) mapResult.get("adPrice");
            adPrices.put(storeId, adPrice);
            adPriceStore.put(storeId, adPrice);
        }
        adPriceStream = AdPriceConfig.AD_PRICE_STREAM;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by userId, which means the messages
        sharing the same userId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
         */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(AdPriceConfig.AD_CLICK_STREAM.getStream())) {
            // Handle Ad-click messages
            Object message = envelope.getMessage();
            if (message instanceof Map) {
                Map<String, Object> adClick = (Map<String, Object>) message;
                Integer userId = (Integer) adClick.get("userId");
                String storeId = (String) adClick.get("storeId");
                String name = (String) adClick.get("name");
                String clicked = (String) adClick.get("clicked");

                // Retrieve adPrice from store
                Integer adPrice = adPriceStore.get(storeId);
                int adShare = 0;
                int cabShare = 0;

                if ("true".equalsIgnoreCase(clicked)) {
                    adShare = (int) (adPrice * 0.8);
                    cabShare = adPrice - adShare;
                } else if ("false".equalsIgnoreCase(clicked)) {
                    adShare = adPrice / 2;
                    cabShare = adPrice - adShare;
                }
                // Create output message
                Map<String, Object> adPriceOutput = new HashMap<>();
                adPriceOutput.put("userId", userId);
                adPriceOutput.put("storeId", storeId);
                adPriceOutput.put("ad", adShare);
                adPriceOutput.put("cab", cabShare);
                try {
                    collector.send(new OutgoingMessageEnvelope(adPriceStream, adPriceOutput));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }
}
