package io.blockchainetl.anomaloustransactions.service;

import java.math.BigInteger;

public interface BigQueryService {

    BigInteger getEtherValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays);
    BigInteger getEtherGasCostThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays);
    BigInteger getBitcoinInputValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays);
}
