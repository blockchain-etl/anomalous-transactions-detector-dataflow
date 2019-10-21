package io.blockchainetl.anomaloustransactions.domain;

import com.google.common.base.Objects;
import io.blockchainetl.anomaloustransactions.domain.ethereum.Transaction;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

import java.math.BigInteger;

@DefaultCoder(AvroCoder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class EthereumAnomalousGasCostMessage {

    @Nullable
    private String type = "ethereum_anomalous_gas_cost";

    @Nullable
    private Transaction transaction;

    @Nullable
    @JsonProperty("gas_cost")
    private BigInteger gasCost;

    @Nullable
    @JsonProperty("period_in_days")
    private Integer periodInDays;

    @Nullable
    @JsonProperty("number_of_transactions_above_threshold")
    private Integer numberOfTransactionsAboveThreshold;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public BigInteger getGasCost() {
        return gasCost;
    }

    public void setGasCost(BigInteger gasCost) {
        this.gasCost = gasCost;
    }

    public Integer getPeriodInDays() {
        return periodInDays;
    }

    public void setPeriodInDays(Integer periodInDays) {
        this.periodInDays = periodInDays;
    }

    public Integer getNumberOfTransactionsAboveThreshold() {
        return numberOfTransactionsAboveThreshold;
    }

    public void setNumberOfTransactionsAboveThreshold(Integer numberOfTransactionsAboveThreshold) {
        this.numberOfTransactionsAboveThreshold = numberOfTransactionsAboveThreshold;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("type", type)
            .add("transaction", transaction)
            .add("gasCost", gasCost)
            .add("periodInDays", periodInDays)
            .add("numberOfTransactionsAboveThreshold", numberOfTransactionsAboveThreshold)
            .toString();
    }
}
