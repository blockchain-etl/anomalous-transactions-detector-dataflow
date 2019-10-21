package io.blockchainetl.anomaloustransactions;

import io.blockchainetl.anomaloustransactions.domain.Transaction;
import io.blockchainetl.anomaloustransactions.fns.AddTimestampsFn;
import io.blockchainetl.anomaloustransactions.fns.EncodeToJsonFn;
import io.blockchainetl.anomaloustransactions.fns.FilterByEtherValueFn;
import io.blockchainetl.anomaloustransactions.fns.FilterByGasCostFn;
import io.blockchainetl.anomaloustransactions.fns.LogElementsFn;
import io.blockchainetl.anomaloustransactions.fns.ParseEntitiesFromJsonFn;
import io.blockchainetl.anomaloustransactions.service.BigQueryServiceHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;


public class AnomalousTransactionsPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalousTransactionsPipeline.class);

    private static final String PUBSUB_ID_ATTRIBUTE = "item_id";

    public static void main(String[] args) throws IOException, InterruptedException {
        AnomalousTransactionsPipelineOptions options =
            PipelineOptionsFactory.fromArgs(args).withValidation().as(AnomalousTransactionsPipelineOptions.class);

        runPipeline(options);
    }

    public static void runPipeline(
        AnomalousTransactionsPipelineOptions options
    ) {
        Pipeline p = Pipeline.create(options);

        // Read input

        PCollection<String> input = p.apply("ReadFromPubSub",
            PubsubIO.readStrings()
                .fromSubscription(options.getInputSubscription())
                .withIdAttribute(PUBSUB_ID_ATTRIBUTE));

        // Build pipeline

        PCollection<String> output = buildPipeline(p, input);

        // Write output

        output
            .apply("LogElements", ParDo.of(new LogElementsFn<>("Message: ")))
            .apply("WriteElements", PubsubIO.writeStrings().to(options.getOutputTopic()));

        // Run pipeline

        PipelineResult pipelineResult = p.run();
        LOG.info(pipelineResult.toString());
        pipelineResult.waitUntilFinish();
    }

    public static PCollection<String> buildPipeline(Pipeline p, PCollection<String> input) {
        PCollectionView<BigInteger> etherValueThreshold = etherValueThreshold(p);
        PCollection<String> etherValueOutput = buildFilterPipeline("EtherValue", etherValueThreshold, 
            new FilterByEtherValueFn(etherValueThreshold), input);

        PCollectionView<BigInteger> gasCostThreshold = gasCostThreshold(p);
        PCollection<String> gasCostOutput = buildFilterPipeline("GasCost", gasCostThreshold,
            new FilterByGasCostFn(gasCostThreshold), input);

        // Combine 

        PCollectionList<String> outputs = PCollectionList.of(etherValueOutput).and(gasCostOutput);
        return outputs.apply("Flatten", Flatten.pCollections());
    }

    public static PCollection<String> buildFilterPipeline(
        String prefix,
        PCollectionView<BigInteger> filterSideInput,
        DoFn<Transaction, ?> filterFn,
        PCollection<String> input
    ) {
        // Add timestamps
        
        PCollection<String> inputWithTimestamps = input
            .apply(prefix + "AddTimestamps", ParDo.of(new AddTimestampsFn()));

        // Parse transactions

        PCollection<Transaction> transactions = inputWithTimestamps
            .apply(prefix + "ParseTransactions", ParDo.of(new ParseEntitiesFromJsonFn<>(Transaction.class)))
            .setCoder(AvroCoder.of(Transaction.class));

        // Filter 

        PCollection<?> filteredTransactions = transactions
            .apply(prefix + "FilterEtherValue", ParDo.of(filterFn).withSideInputs(filterSideInput));

        // Encode to JSON

        return filteredTransactions.apply(prefix + "EncodeToJson", ParDo.of(new EncodeToJsonFn()));
    }

    // https://beam.apache.org/documentation/patterns/side-input-patterns/#using-global-window-side-inputs-in-non
    // -global-windows
    public static PCollectionView<BigInteger> etherValueThreshold(Pipeline p) {
        PCollectionView<BigInteger> etherValueThreshold =
            p.apply("GenerateSequenceForEtherValue", GenerateSequence.from(0).withRate(1, Duration.standardHours(24L)))
                .apply("WindowForEtherValue", Window.<Long>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes()
                )
                .apply("QueryEtherValue", 
                    ParDo.of(
                        new DoFn<Long, BigInteger>() {

                            @ProcessElement
                            public void process(@Element Long input, DoFn.OutputReceiver<BigInteger> o) {
                                o.output(BigQueryServiceHolder.INSTANCE.getEtherValueThreshold(
                                    Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD, Constants.PERIOD_IN_DAYS));
                            }
                        }))
                .apply("SingletonForEtherValue", View.asSingleton());
        return etherValueThreshold;
    }

    public static PCollectionView<BigInteger> gasCostThreshold(Pipeline p) {
        PCollectionView<BigInteger> gasCostThreshold =
            p.apply("GenerateSequenceForGasCost", GenerateSequence.from(0).withRate(1, Duration.standardHours(24L)))
                .apply("WindowForGasCost", Window.<Long>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes()
                )
                .apply("QueryGasCost",
                    ParDo.of(
                        new DoFn<Long, BigInteger>() {

                            @ProcessElement
                            public void process(@Element Long input, DoFn.OutputReceiver<BigInteger> o) {
                                o.output(BigQueryServiceHolder.INSTANCE.getGasCostThreshold(
                                    Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD, Constants.PERIOD_IN_DAYS));
                            }
                        }))
                .apply("SingletonForGasCost", View.asSingleton());
        return gasCostThreshold;
    }
}
