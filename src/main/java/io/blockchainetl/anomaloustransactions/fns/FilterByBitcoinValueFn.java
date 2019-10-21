package io.blockchainetl.anomaloustransactions.fns;

import io.blockchainetl.anomaloustransactions.Constants;
import io.blockchainetl.anomaloustransactions.domain.BitcoinAnomalousValueMessage;
import io.blockchainetl.anomaloustransactions.domain.bitcoin.Transaction;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;

public class FilterByBitcoinValueFn extends ErrorHandlingDoFn<Transaction, BitcoinAnomalousValueMessage> {

    private final PCollectionView<BigInteger> bitcoinValueThresholdSideInput;

    public FilterByBitcoinValueFn(PCollectionView<BigInteger> bitcoinValueThresholdSideInput) {
        this.bitcoinValueThresholdSideInput = bitcoinValueThresholdSideInput;
    }

    @Override
    protected void doProcessElement(ProcessContext c) {
        BigInteger bitcoinValueThreshold = c.sideInput(this.bitcoinValueThresholdSideInput);

        Transaction transaction = c.element();
        BigInteger value = transaction.getInputValue();

        if (value != null && value.compareTo(bitcoinValueThreshold) >= 0) {
            BitcoinAnomalousValueMessage message = new BitcoinAnomalousValueMessage();

            message.setTransaction(transaction);
            message.setNumberOfTransactionsAboveThreshold(Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD);
            message.setPeriodInDays(Constants.PERIOD_IN_DAYS);
            c.output(message);
        }
    }
}
