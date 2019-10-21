package io.blockchainetl.anomaloustransactions;

import io.blockchainetl.anomaloustransactions.fns.LogElementsFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AnomalousTransactionsPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalousTransactionsPipeline.class);

    private static final String PUBSUB_ID_ATTRIBUTE = "item_id";

    public static void main(String[] args) {
        AnomalousTransactionsPipelineOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(AnomalousTransactionsPipelineOptions.class);

        runPipeline(options);
    }

    public static void runPipeline(
        AnomalousTransactionsPipelineOptions options
    ) {
        Pipeline p = Pipeline.create(options);

        // Build Ethereum pipeline

        PCollection<String> ethereumTransactionsInput = p.apply("ReadEthereumTransactionsFromPubSub",
            PubsubIO.readStrings()
                .fromSubscription(options.getEthereumTransactionsSubscription())
                .withIdAttribute(PUBSUB_ID_ATTRIBUTE));

        PCollection<String> ethereumOutput = EthereumPipeline.buildEthereumPipeline(p, ethereumTransactionsInput);

        // Build Bitcoin pipeline

        PCollection<String> bitcoinTransactionsInput = p.apply("ReadBitcoinTransactionsFromPubSub",
            PubsubIO.readStrings()
                .fromSubscription(options.getEthereumTransactionsSubscription())
                .withIdAttribute(PUBSUB_ID_ATTRIBUTE));
        
        PCollection<String> bitcoinOutput = BitcoinPipeline.buildBitcoinPipeline(p, bitcoinTransactionsInput);

        // Write output

        PCollectionList<String> outputs = PCollectionList.of(ethereumOutput).and(bitcoinOutput);
        PCollection<String> flattenOutputs = outputs.apply("Flatten", Flatten.pCollections());

        flattenOutputs
            .apply("LogElements", ParDo.of(new LogElementsFn<>("Message: ")))
            .apply("WriteElements", PubsubIO.writeStrings().to(options.getOutputTopic()));

        // Run pipeline

        PipelineResult pipelineResult = p.run();
        LOG.info(pipelineResult.toString());
        pipelineResult.waitUntilFinish();
    }
}
