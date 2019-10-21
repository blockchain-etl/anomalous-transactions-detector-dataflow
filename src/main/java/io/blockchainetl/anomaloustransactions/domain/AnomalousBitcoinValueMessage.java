package io.blockchainetl.anomaloustransactions.domain;

import com.google.common.base.Objects;
import io.blockchainetl.anomaloustransactions.domain.bitcoin.Transaction;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;

@DefaultCoder(AvroCoder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AnomalousBitcoinValueMessage {

    @Nullable
    private String type = "anomalous_bitcoin_value";

    @Nullable
    private Transaction transaction;

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
            .add("periodInDays", periodInDays)
            .add("numberOfTransactionAboveThreshold", numberOfTransactionsAboveThreshold)
            .toString();
    }
}
