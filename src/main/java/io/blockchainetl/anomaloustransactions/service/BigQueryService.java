package io.blockchainetl.anomaloustransactions.service;

import java.math.BigInteger;

public interface BigQueryService {

    BigInteger getEthereumValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays);
    BigInteger getEthereumGasCostThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays);
    BigInteger getBitcoinValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays);
}
