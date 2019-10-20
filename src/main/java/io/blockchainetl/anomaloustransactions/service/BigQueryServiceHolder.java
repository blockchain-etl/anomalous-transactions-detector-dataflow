package io.blockchainetl.anomaloustransactions.service;

public class BigQueryServiceHolder {

    public static BigQueryService INSTANCE = new BigQueryServiceImpl();
}
