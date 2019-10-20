package io.blockchainetl.anomaloustransactions.fns;

import io.blockchainetl.anomaloustransactions.Constants;
import io.blockchainetl.anomaloustransactions.domain.AnomalousGasPriceMessage;
import io.blockchainetl.anomaloustransactions.domain.Transaction;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;

public class FilterByGasPriceFn extends ErrorHandlingDoFn<Transaction, AnomalousGasPriceMessage> {

    private final PCollectionView<BigInteger> gasPriceThresholdSideInput;

    public FilterByGasPriceFn(PCollectionView<BigInteger> gasPriceThresholdSideInput) {
        this.gasPriceThresholdSideInput = gasPriceThresholdSideInput;
    }

    @Override
    protected void doProcessElement(ProcessContext c) {
        BigInteger gasPriceThreshold = c.sideInput(this.gasPriceThresholdSideInput);

        Transaction transaction = c.element().clone();
        BigInteger gasPrice = transaction.getGasPrice();

        if (gasPrice.compareTo(gasPriceThreshold) >= 0) {
            AnomalousGasPriceMessage message = new AnomalousGasPriceMessage();

            message.setTransaction(transaction);
            message.setNumberOfTransactionsAboveThreshold(Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD);
            message.setPeriodInDays(Constants.PERIOD_IN_DAYS);
            c.output(message);
        }
    }
}
