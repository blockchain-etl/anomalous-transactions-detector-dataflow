package io.blockchainetl.anomaloustransactions;

import io.blockchainetl.anomaloustransactions.domain.ethereum.Transaction;
import io.blockchainetl.anomaloustransactions.fns.AddTimestampsFn;
import io.blockchainetl.anomaloustransactions.fns.EncodeToJsonFn;
import io.blockchainetl.anomaloustransactions.fns.FilterByEthereumValueFn;
import io.blockchainetl.anomaloustransactions.fns.FilterByEthereumGasCostFn;
import io.blockchainetl.anomaloustransactions.fns.ParseEntitiesFromJsonFn;
import io.blockchainetl.anomaloustransactions.service.BigQueryServiceHolder;
import io.blockchainetl.anomaloustransactions.utils.DataflowUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;


public class EthereumPipeline {

    public static PCollection<String> buildEthereumPipeline(Pipeline p, PCollection<String> input) {
        PCollectionView<BigInteger> etherValueThreshold = valueThreshold(p);
        PCollection<String> etherValueOutput = buildFilterEthereumPipeline("EtherValue", etherValueThreshold,
            new FilterByEthereumValueFn(etherValueThreshold), input);

        PCollectionView<BigInteger> gasCostThreshold = gasCostThreshold(p);
        PCollection<String> gasCostOutput = buildFilterEthereumPipeline("GasCost", gasCostThreshold,
            new FilterByEthereumGasCostFn(gasCostThreshold), input);

        // Combine 

        PCollectionList<String> outputs = PCollectionList.of(etherValueOutput).and(gasCostOutput);
        return outputs.apply("Flatten", Flatten.pCollections());
    }

    public static PCollection<String> buildFilterEthereumPipeline(
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

    private static PCollectionView<BigInteger> valueThreshold(Pipeline p) {
        return DataflowUtils.getPCollectionViewForValue(p, "EthereumValue", new DoFn<Long, BigInteger>() {
            @ProcessElement
            public void process(@Element Long input, OutputReceiver<BigInteger> o) {
                o.output(BigQueryServiceHolder.INSTANCE.getEthereumValueThreshold(
                    Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD, Constants.PERIOD_IN_DAYS));
            }
        });
    }

    private static PCollectionView<BigInteger> gasCostThreshold(Pipeline p) {
        return DataflowUtils.getPCollectionViewForValue(p, "EthereumGasCost", new DoFn<Long, BigInteger>() {
                @ProcessElement
                public void process(@Element Long input, OutputReceiver<BigInteger> o) {
                    o.output(BigQueryServiceHolder.INSTANCE.getEthereumGasCostThreshold(
                        Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD, Constants.PERIOD_IN_DAYS));
                }
            }
        );
    }
}
