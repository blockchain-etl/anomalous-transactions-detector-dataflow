package io.blockchainetl.anomaloustransactions.fns;

import io.blockchainetl.anomaloustransactions.Constants;
import io.blockchainetl.anomaloustransactions.domain.AnomalousGasCostMessage;
import io.blockchainetl.anomaloustransactions.domain.Transaction;
import org.apache.beam.sdk.values.PCollectionView;

import java.math.BigInteger;

public class FilterByGasCostFn extends ErrorHandlingDoFn<Transaction, AnomalousGasCostMessage> {

    private final PCollectionView<BigInteger> gasCostThresholdSideInput;

    public FilterByGasCostFn(PCollectionView<BigInteger> gasCostThresholdSideInput) {
        this.gasCostThresholdSideInput = gasCostThresholdSideInput;
    }

    @Override
    protected void doProcessElement(ProcessContext c) {
        BigInteger gasCostThreshold = c.sideInput(this.gasCostThresholdSideInput);

        Transaction transaction = c.element().clone();
        BigInteger gasPrice = transaction.getGasPrice();
        Long gasUsed = transaction.getReceiptGasUsed();
        BigInteger gasCost = gasPrice.multiply(BigInteger.valueOf(gasUsed));

        if (gasCost.compareTo(gasCostThreshold) >= 0) {
            AnomalousGasCostMessage message = new AnomalousGasCostMessage();

            message.setTransaction(transaction);
            message.setNumberOfTransactionsAboveThreshold(Constants.NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD);
            message.setPeriodInDays(Constants.PERIOD_IN_DAYS);
            message.setGasCost(gasCost);
            c.output(message);
        }
    }
}
