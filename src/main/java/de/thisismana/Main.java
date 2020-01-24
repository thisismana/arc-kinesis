package de.thisismana;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.*;

import java.util.concurrent.ExecutionException;

class Main {

    private static final String STREAM_NAME = "com.arcpublishing.stroeer.contentv2.ans.v3";

    private static DescribeStreamResponse describe_stream(KinesisAsyncClient client) throws ExecutionException, InterruptedException {
        var request = DescribeStreamRequest.builder()
                .streamName(STREAM_NAME)
                .build();
        return client.describeStream(request).get();
    }

    private static GetShardIteratorResponse shard_iterator(KinesisAsyncClient client, String stream_name) throws ExecutionException, InterruptedException {
        System.out.println("shard_iterator(" + stream_name + ")");
        var request = GetShardIteratorRequest.builder()
                .streamName(stream_name)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                .shardId("shardId-000000000000")
                .build();

        return client.getShardIterator(request).get();
    }

    private static GetRecordsResponse records(KinesisAsyncClient client, String shard_iterator) throws ExecutionException, InterruptedException {
        var request = GetRecordsRequest.builder()
                .shardIterator(shard_iterator)
                .build();
        return client.getRecords(request).get();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        KinesisAsyncClient client = KinesisAsyncClient.builder()
                .region(Region.EU_CENTRAL_1)
                .credentialsProvider(ProfileCredentialsProvider.create("arc"))
                .build();

        DescribeStreamResponse describeStreamResponse = describe_stream(client);
        String stream_arn = describeStreamResponse.streamDescription().streamARN();
        String stream_name = describeStreamResponse.streamDescription().streamName();

        System.out.println("stream arn  = " + stream_arn);
        System.out.println("stream name = " + stream_name);

        GetShardIteratorResponse getShardIteratorResponse = shard_iterator(client, stream_name);

        String shard_iterator = getShardIteratorResponse.shardIterator();
        System.out.println("shard_iterator = " + shard_iterator);

        GetRecordsResponse records = records(client, shard_iterator);

        System.out.println("Records [" + records.records().size() + "]");

        records.records().forEach(record -> {
            System.out.println("\tseq_nr = " + record.sequenceNumber());
            System.out.println("\tdata   = " + record.data().toString());
        });

    }
}