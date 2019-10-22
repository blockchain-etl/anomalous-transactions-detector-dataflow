package io.blockchainetl.anomaloustransactions.service;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.blockchainetl.anomaloustransactions.Constants.SATOSHI_IN_ONE_BITCOIN;
import static io.blockchainetl.anomaloustransactions.Constants.WEI_IN_ONE_ETHER;

public class BigQueryServiceImpl implements BigQueryService {

    private static final Logger LOG = LoggerFactory.getLogger(BigQueryServiceImpl.class);
    
    private static final BigInteger DEFAULT_ETHER_VALUE_THRESHOLD = new BigInteger("1000").multiply(WEI_IN_ONE_ETHER);
    private static final BigInteger DEFAULT_ETHER_GAS_COST_THRESHOLD = new BigInteger("10").multiply(WEI_IN_ONE_ETHER);
    private static final BigInteger DEFAULT_BITCOIN_VALUE_THRESHOLD = new BigInteger("10000").multiply(SATOSHI_IN_ONE_BITCOIN);

    @Override
    public BigInteger getEthereumValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays) {
        String query = String.format("select value\n"
            + "from `bigquery-public-data.crypto_ethereum.transactions` as t\n"
            + "where DATE(block_timestamp) > DATE_ADD(CURRENT_DATE() , INTERVAL -%s DAY)\n"
            + "order by value desc\n"
            + "limit %s", periodInDays, numberOfTransactionsAboveThreshold);

        BigInteger result = getLastValueFromQuery(query, "value", DEFAULT_ETHER_VALUE_THRESHOLD);

        LOG.info("Ether value threshold is: " + result.toString());
        
        return result;
    }

    @Override
    public BigInteger getEthereumGasCostThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays) {
        String query = String.format("select gas_price * receipt_gas_used as gas_cost\n"
            + "from `bigquery-public-data.crypto_ethereum.transactions` as t\n"
            + "where DATE(block_timestamp) > DATE_ADD(CURRENT_DATE() , INTERVAL -%s DAY)\n"
            + "order by gas_cost desc\n"
            + "limit %s", periodInDays, numberOfTransactionsAboveThreshold);

        BigInteger result = getLastValueFromQuery(query, "gas_cost", DEFAULT_ETHER_GAS_COST_THRESHOLD);

        LOG.info("Gas cost threshold threshold is: " + result.toString());
        
        return result;
    }

    @Override
    public BigInteger getBitcoinValueThreshold(Integer numberOfTransactionsAboveThreshold, Integer periodInDays) {
        String query = String.format("select input_value\n"
            + "from `bigquery-public-data.crypto_bitcoin.transactions` as t\n"
            + "where DATE(block_timestamp) > DATE_ADD(CURRENT_DATE() , INTERVAL -%s DAY)\n"
            + "and block_timestamp_month >= DATE_TRUNC(DATE_ADD(CURRENT_DATE(), INTERVAL -%s DAY), MONTH)"
            + "order by input_value desc\n"
            + "limit %s", periodInDays, numberOfTransactionsAboveThreshold, numberOfTransactionsAboveThreshold);

        BigInteger result = getLastValueFromQuery(query, "input_value", DEFAULT_BITCOIN_VALUE_THRESHOLD);

        LOG.info("Bitcoin input value threshold is: " + result.toString());

        return result;
    }

    private BigInteger getLastValueFromQuery(String query, String fieldName, BigInteger defaultValue) {
        QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
        TableResult tableResult;
        try {
            LOG.info("Calling BigQuery: " + query);
            tableResult = bigquery.query(queryConfig);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Got results with size: " + tableResult.getTotalRows());

        BigInteger result = getLastValueFromTableResult(tableResult, fieldName);

        if (result == null) {
            LOG.info("No rows in BigQuery results. Using default value.");
            result = defaultValue;
        }
        
        return result;
    }

    private BigInteger getLastValueFromTableResult(TableResult tableResult, String fieldName) {
        BigInteger result = null;
        for (FieldValueList row : tableResult.iterateAll()) {
            FieldValue value = row.get(fieldName);

            if (value == null || value.getNumericValue() == null) {
                throw new IllegalArgumentException("Value is null in table result.");
            }

            BigDecimal numericValue = value.getNumericValue();

            result = numericValue.toBigInteger();
        }
        return result;
    }
}
