package com.cloudcomputing.samza.nycabs;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;

import com.cloudcomputing.samza.nycabs.application.AdPriceTaskApplication;

public class TestAdPriceTask {

    /**
     * Test Case 1: Verify that when "clicked" is "true", the ad company
     * receives 80% of adPrice, and NYCabs receives 20% of adPrice.
     */
    @Test
    public void testClickedTrue() throws Exception {
        // Initialize configuration map
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.ad-price-store.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.ad-price-store.key.serde", "string");
        confMap.put("stores.ad-price-store.msg.serde", "integer");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
        confMap.put("serializers.registry.integer.class", "org.apache.samza.serializers.IntegerSerdeFactory");

        // Set up in-memory system
        InMemorySystemDescriptor systemDescriptor = new InMemorySystemDescriptor("test-kafka");

        // Define input and output descriptors
        InMemoryInputDescriptor<Map<String, Object>> inputDescriptor
                = systemDescriptor.getInputDescriptor("ad-click", new NoOpSerde<>());
        InMemoryOutputDescriptor<Map<String, Object>> outputDescriptor
                = systemDescriptor.getOutputDescriptor("ad-price", new NoOpSerde<>());

        // Initialize TestRunner
        TestRunner
                .of(new AdPriceTaskApplication())
                .addInputStream(inputDescriptor, TestUtils.genStreamData("adClickTrue.json"))
                .addOutputStream(outputDescriptor, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true") // Ensure test mode
                .run(Duration.ofSeconds(7));

// Consume the output stream
        List<Object> outputMessages = TestRunner.consumeStream(outputDescriptor, Duration.ofSeconds(7)).get(0);

// Assert that exactly one message is produced
        Assert.assertEquals("Expected exactly one output message", 1, outputMessages.size());

// Iterate through the output messages
        Iterator<Object> iterator = outputMessages.iterator();
        while (iterator.hasNext()) {
            Object message = iterator.next();
            Map<String, Object> adPriceOutput = (Map<String, Object>) message;

            // Verify the fields
            Assert.assertEquals("User ID should be 1", 1, adPriceOutput.get("userId"));
            Assert.assertEquals("Store ID should be store123", "store123", adPriceOutput.get("storeId"));
            Assert.assertEquals("Ad share should be 80", 80, adPriceOutput.get("ad"));
            Assert.assertEquals("Cab share should be 20", 20, adPriceOutput.get("cab"));
        }
    }

    /**
     * Test Case 2: Verify that when "clicked" is "false", both the ad company
     * and NYCabs receive 50% of adPrice.
     */
    @Test
    public void testClickedFalse() throws Exception {
        // Initialize configuration map
        Map<String, String> confMap = new HashMap<>();
        confMap.put("stores.ad-price-store.factory", "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory");
        confMap.put("stores.ad-price-store.key.serde", "string");
        confMap.put("stores.ad-price-store.msg.serde", "integer");
        confMap.put("serializers.registry.json.class", "org.apache.samza.serializers.JsonSerdeFactory");
        confMap.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
        confMap.put("serializers.registry.integer.class", "org.apache.samza.serializers.IntegerSerdeFactory");

        // Set up in-memory system
        InMemorySystemDescriptor systemDescriptor = new InMemorySystemDescriptor("test-kafka");

        // Define input and output descriptors
        InMemoryInputDescriptor<Map<String, Object>> inputDescriptor
                = systemDescriptor.getInputDescriptor("ad-click", new NoOpSerde<>());
        InMemoryOutputDescriptor<Map<String, Object>> outputDescriptor
                = systemDescriptor.getOutputDescriptor("ad-price", new NoOpSerde<>());

        // Initialize TestRunner
        TestRunner
                .of(new AdPriceTaskApplication())
                .addInputStream(inputDescriptor, TestUtils.genStreamData("adClickFalse.json"))
                .addOutputStream(outputDescriptor, 1)
                .addConfig(confMap)
                .addConfig("deploy.test", "true") // Ensure test mode
                .run(Duration.ofSeconds(7));

        // Consume the output stream
        List<Object> outputMessages = TestRunner.consumeStream(outputDescriptor, Duration.ofSeconds(7)).get(0);

        // Assert that exactly one message is produced
        Assert.assertEquals("Expected exactly one output message", 1, outputMessages.size());

        // Iterate through the output messages
        Iterator<Object> iterator = outputMessages.iterator();
        while (iterator.hasNext()) {
            Object message = iterator.next();
            Map<String, Object> adPriceOutput = (Map<String, Object>) message;

            // Verify the fields
            Assert.assertEquals("User ID should be 2", 2, adPriceOutput.get("userId"));
            Assert.assertEquals("Store ID should be store456", "store456", adPriceOutput.get("storeId"));
            Assert.assertEquals("Ad share should be 50", 50, adPriceOutput.get("ad"));
            Assert.assertEquals("Cab share should be 50", 50, adPriceOutput.get("cab"));
        }
    }
}
