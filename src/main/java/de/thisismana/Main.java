package de.thisismana;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

class Main {

    private static final String STREAM_NAME = "com.arcpublishing.stroeer.contentv2.ans.v3";
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

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

        return shards.stream().map(shard -> {

            var request = SubscribeToShardRequest.builder()
                    .consumerARN(stream_arn + "/consumer/mana_stream:0")
                    .shardId(shard.shardId())
                    .startingPosition(s -> s.type(ShardIteratorType.LATEST)).build();

            var handler = SubscribeToShardResponseHandler.builder()
                    .onComplete(() -> logger.info("Complete"))
                    .onError(e -> logger.error("Handler error.", e))
                    .subscriber(e -> logger.info("Received event - " + e))
                    .build();

            return client.subscribeToShard(request, handler);

        }).collect(Collectors.toList());

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
                .exceptionally(th -> {
                            logger.error("Subscriber failed", th);
                            return null;
                        }
                )
                .get();

        logger.info("done");
        client.close();
    }
}