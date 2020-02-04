package de.thisismana;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;
import software.amazon.awssdk.services.kinesis.model.SubscribeToShardResponseHandler.Visitor;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.time.Instant;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

class Main {

    public static final String STREAM_NAME = "com.arcpublishing.sandbox.stroeer.contentv2.ans.v3";
    public static final String STREAM_CONSUMER_NAME = "mana";
    public static final CharsetDecoder DECODER = Charset.forName("UTF-8").newDecoder();
    public static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static String decompress(byte[] compressed) throws IOException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(compressed);
             GZIPInputStream gis = new GZIPInputStream(bis);
             BufferedReader br = new BufferedReader(new InputStreamReader(gis, "UTF-8"))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    private static DescribeStreamResponse describe_stream(KinesisAsyncClient client) throws ExecutionException, InterruptedException {
        var request = DescribeStreamRequest.builder()
                .streamName(STREAM_NAME)
                .build();
        return client.describeStream(request).get();
    }

    private static GetShardIteratorResponse shard_iterator(KinesisAsyncClient client, String stream_name, String shard_id) throws ExecutionException, InterruptedException {
        var request = GetShardIteratorRequest.builder()
                .streamName(stream_name)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .shardId(shard_id)
                .build();

        return client.getShardIterator(request).get();
    }

    private static GetRecordsResponse records(KinesisAsyncClient client, String shard_iterator) throws ExecutionException, InterruptedException {
        var request = GetRecordsRequest.builder()
                .shardIterator(shard_iterator)
                .build();
        return client.getRecords(request).get();
    }

    private static List<Shard> shard_ids(KinesisAsyncClient client, String stream_name) throws ExecutionException, InterruptedException {
        var request = ListShardsRequest.builder()
                .streamName(stream_name)
                .build();
        return client.listShards(request).get().shards();
    }

    private static List<CompletableFuture<Void>> subscribe(KinesisAsyncClient client, String stream_arn, Collection<Shard> shards) {

        create_stream_consumer(client, stream_arn);
        final Instant timestamp = describe_stream_consumer(client, stream_arn);

        return shards.stream().map(shard -> {
            var request = SubscribeToShardRequest.builder()
                    .consumerARN(String.format("%s/consumer/%s:%d", stream_arn, STREAM_CONSUMER_NAME, timestamp.getEpochSecond()))
                    .shardId(shard.shardId())
                    .startingPosition(s -> s.type(ShardIteratorType.LATEST)).build();

            var handler = SubscribeToShardResponseHandler.builder()
                    .onComplete(() -> logger.info("Complete"))
                    .onError(e -> logger.error("Handler error.", e))
                    .subscriber((SubscribeToShardEventStream e) -> {
                        e.accept(new Visitor() {
                            @Override
                            public void visitDefault(SubscribeToShardEventStream event) {

                            }

                            @Override
                            public void visit(SubscribeToShardEvent event) {
                                logger.info("Event [raw] = {}", event);
                                event.records().forEach((Record record) -> {
                                    String data = "";
                                    try {
                                        data = decompress(record.data().asByteArray());
                                    } catch (IOException ex) {
                                        logger.error("DECODING FAILED.", ex);
                                    }
                                    logger.info(data);
                                });
                            }
                        });
                    })
                    .build();

            logger.info("Adding subscription for shard={}", shard.shardId());
            return client.subscribeToShard(request, handler);

        }).collect(Collectors.toList());

    }

    private static void create_stream_consumer(KinesisAsyncClient client, String stream_arn) {
        try {
            var rscr = RegisterStreamConsumerRequest.builder()
                    .consumerName(STREAM_CONSUMER_NAME)
                    .streamARN(stream_arn)
                    .build();
            client.registerStreamConsumer(rscr).get();
        } catch (ExecutionException ignored) {
            logger.info("Consumer '" + STREAM_CONSUMER_NAME + "' exists.");
        } catch (InterruptedException e) {
            logger.error("Error.", e);
        }
    }

    private static Instant describe_stream_consumer(KinesisAsyncClient client, String stream_arn) {
        try {
            var dscr = DescribeStreamConsumerRequest.builder()
                    .streamARN(stream_arn)
                    .consumerName(STREAM_CONSUMER_NAME)
                    .build();
            DescribeStreamConsumerResponse describeStreamConsumerResponse = client.describeStreamConsumer(dscr).get();
            ConsumerDescription arg = describeStreamConsumerResponse.consumerDescription();
            logger.info("Consumer desc.: {}", arg);
            return arg.consumerCreationTimestamp();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error.", e);
        }
        return Instant.MIN;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KinesisAsyncClient client = KinesisAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .credentialsProvider(ProfileCredentialsProvider.create("arc"))
                .build();

        DescribeStreamResponse describeStreamResponse = describe_stream(client);
        String stream_arn = describeStreamResponse.streamDescription().streamARN();
        String stream_name = describeStreamResponse.streamDescription().streamName();
        List<Shard> shards = shard_ids(client, stream_name);


        logger.info("stream arn  = " + stream_arn);
        logger.info("stream name = " + stream_name);

        logger.info("Found Shards [{}]", shards.stream().map(Shard::shardId).collect(Collectors.joining(", ", "(", ")")));

        for (Shard shard : shards) {
            GetShardIteratorResponse getShardIteratorResponse = shard_iterator(client, stream_name, shard.shardId());

            String shard_iterator = getShardIteratorResponse.shardIterator();
            logger.info("shard_iterator = " + shard_iterator);

            GetRecordsResponse records = records(client, shard_iterator);

            logger.info("Found [" + records.records().size() + "] Records for shard " + shard.shardId());

            records.records().forEach(record -> {
                logger.info("\tseq_nr = " + record.sequenceNumber());
                logger.info("\tdata   = " + record.data().toString());
            });
        }

        CompletableFuture[] futures = subscribe(client, stream_arn, shards).toArray(new CompletableFuture[0]);

        // wait for all subscribers to complete
        CompletableFuture.allOf(futures)
                .exceptionally(th -> null)
                .get();

        logger.info("done");
        client.close();
    }
}