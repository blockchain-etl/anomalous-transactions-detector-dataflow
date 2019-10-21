package io.blockchainetl.anomaloustransactions.fns;

import io.blockchainetl.anomaloustransactions.Constants;
import io.blockchainetl.anomaloustransactions.domain.AnomalousBitcoinValueMessage;
import io.blockchainetl.anomaloustransactions.domain.bitcoin.Transaction;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;

public class FilterByBitcoinValueFn extends ErrorHandlingDoFn<Transaction, AnomalousBitcoinValueMessage> {

    private final PCollectionView<BigInteger> bitcoinInputValueThresholdSideInput;

    public FilterByBitcoinValueFn(PCollectionView<BigInteger> bitcoinInputValueThresholdSideInput) {
        this.bitcoinInputValueThresholdSideInput = bitcoinInputValueThresholdSideInput;
    }

    @Override
    protected void doProcessElement(ProcessContext c) {
        BigInteger bitcoinInputValueThreshold = c.sideInput(this.bitcoinInputValueThresholdSideInput);

        Transaction transaction = c.element();
        BigInteger value = transaction.getInputValue();

        if (value.compareTo(bitcoinInputValueThreshold) >= 0) {
            AnomalousBitcoinValueMessage message = new AnomalousBitcoinValueMessage();

            message.setTransaction(transaction);
            message.setNumberOfTransactionsAboveThreshold(Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD);
            message.setPeriodInDays(Constants.PERIOD_IN_DAYS);
            c.output(message);
        }
    }
}
