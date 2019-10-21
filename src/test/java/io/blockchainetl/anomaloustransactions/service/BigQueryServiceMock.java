package io.blockchainetl.anomaloustransactions.service;

import io.blockchainetl.anomaloustransactions.Constants;

import java.math.BigInteger;

public class BigQueryServiceMock implements BigQueryService {

    @Override
    public BigInteger getEthereumValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays) {
        return Constants.WEI_IN_ONE_ETHER;
    }

    @Override
    public BigInteger getEthereumGasCostThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays) {
        return new BigInteger("2600000000000000");
    }

    @Override
    public BigInteger getBitcoinValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays) {
        return new BigInteger("1400000");
    }
}
