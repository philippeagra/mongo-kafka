package com.mongodb.kafka.connect.sink.processor;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;

public class MetadataTimestampProcessor extends PostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataTimestampProcessor.class);

  public MetadataTimestampProcessor(MongoSinkTopicConfig config) {
    super(config);
  }

  @Override
  public void process(SinkDocument sinkDocument, SinkRecord orig) {
    LOGGER.debug("Processing sink document: {}", sinkDocument);
    sinkDocument.getValueDoc().ifPresent(this::withMetadataTimestamp);
  }

  private void withMetadataTimestamp(BsonDocument doc) {
    try {
      BsonDocument metadata = doc.getDocument("metadata");
      BsonString timestamp = metadata.getString("timestamp");
      Date from = Date.from(Instant.parse(timestamp.getValue()));
      BsonDateTime bsonDateTime = new BsonDateTime(from.getTime());
      metadata.put("timestamp", bsonDateTime);
    } catch (Exception e) {
      LOGGER.error("Error while processing sink document: {}", e.getMessage());
    }
  }
}
