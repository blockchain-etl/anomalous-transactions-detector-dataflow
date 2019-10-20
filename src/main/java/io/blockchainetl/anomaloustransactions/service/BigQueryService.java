package io.blockchainetl.anomaloustransactions.service;

import java.math.BigInteger;

public interface BigQueryService {

    BigInteger getEtherValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays);
    BigInteger getGasPriceThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays);
}
