package io.blockchainetl.anomaloustransactions;

import io.blockchainetl.anomaloustransactions.domain.bitcoin.Transaction;
import io.blockchainetl.anomaloustransactions.fns.AddTimestampsFn;
import io.blockchainetl.anomaloustransactions.fns.EncodeToJsonFn;
import io.blockchainetl.anomaloustransactions.fns.FilterByBitcoinValueFn;
import io.blockchainetl.anomaloustransactions.fns.ParseEntitiesFromJsonFn;
import io.blockchainetl.anomaloustransactions.service.BigQueryServiceHolder;
import io.blockchainetl.anomaloustransactions.utils.DataflowUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;


public class BitcoinPipeline {

    public static PCollection<String> buildBitcoinPipeline(Pipeline p, PCollection<String> input) {
        PCollectionView<BigInteger> bitcoinValueThreshold = bitcoinInputValueThreshold(p);
        PCollection<String> bitcoinValueOutput = buildFilterBitcoinPipeline("BitcoinInputValue", bitcoinValueThreshold,
            new FilterByBitcoinValueFn(bitcoinValueThreshold), input);

        return bitcoinValueOutput;
    }

    public static PCollection<String> buildFilterBitcoinPipeline(
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

    private static PCollectionView<BigInteger> bitcoinInputValueThreshold(Pipeline p) {
        return DataflowUtils.getPCollectionViewForValue(p, "BitcoinInputValue", new DoFn<Long, BigInteger>() {
                @ProcessElement
                public void process(@Element Long input, OutputReceiver<BigInteger> o) {
                    o.output(BigQueryServiceHolder.INSTANCE.getBitcoinInputValueThreshold(
                        Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD, Constants.PERIOD_IN_DAYS));
                }
            }
        );
    }
}
