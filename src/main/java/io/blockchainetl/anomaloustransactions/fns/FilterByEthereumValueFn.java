package io.blockchainetl.anomaloustransactions.fns;

import io.blockchainetl.anomaloustransactions.Constants;
import io.blockchainetl.anomaloustransactions.domain.EthereumAnomalousValueMessage;
import io.blockchainetl.anomaloustransactions.domain.ethereum.Transaction;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;

public class FilterByEthereumValueFn extends ErrorHandlingDoFn<Transaction, EthereumAnomalousValueMessage> {

    private final PCollectionView<BigInteger> etherValueThresholdSideInput;

    public FilterByEthereumValueFn(PCollectionView<BigInteger> etherValueThresholdSideInput) {
        this.etherValueThresholdSideInput = etherValueThresholdSideInput;
    }

    @Override
    protected void doProcessElement(ProcessContext c) {
        BigInteger etherValueThreshold = c.sideInput(this.etherValueThresholdSideInput);

        Transaction transaction = c.element().clone();
        BigInteger value = transaction.getValue();

        if (value != null && value.compareTo(etherValueThreshold) >= 0) {
            EthereumAnomalousValueMessage message = new EthereumAnomalousValueMessage();

            message.setTransaction(transaction);
            message.setNumberOfTransactionsAboveThreshold(Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD);
            message.setPeriodInDays(Constants.PERIOD_IN_DAYS);
            c.output(message);
        }
    }
}
