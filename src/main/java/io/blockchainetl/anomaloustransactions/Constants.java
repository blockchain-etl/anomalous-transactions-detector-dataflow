package io.blockchainetl.anomaloustransactions;

import java.math.BigInteger;

public class Constants {
    
    public static final Integer NUMBER_OF_TRANSACTIONS_ABOVE_THRESHOLD = 2;
    public static final Integer PERIOD_IN_DAYS = 7;

    public static final BigInteger WEI_IN_ONE_ETHER = new BigInteger("1000000000000000000");
    public static final BigInteger SATOSHI_IN_ONE_BITCOIN = new BigInteger("100000000");
}
