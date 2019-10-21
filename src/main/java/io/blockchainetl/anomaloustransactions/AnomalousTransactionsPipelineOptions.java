package io.blockchainetl.anomaloustransactions;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;

public interface AnomalousTransactionsPipelineOptions extends PipelineOptions, StreamingOptions, SdkHarnessOptions,
    DataflowWorkerHarnessOptions {

    @Description("Input PubSub subscription for Ethereum Transactions")
    @Validation.Required
    String getEthereumTransactionsSubscription();

    void setEthereumTransactionsSubscription(String value);

    @Description("Input PubSub subscription for Bitcoin Transactions")
    @Validation.Required
    String getBitcoinTransactionsSubscription();

    void setBitcoinTransactionsSubscription(String value);
    
    @Description("Output PubSub topic")
    @Validation.Required
    String getOutputTopic();

    void setOutputTopic(String value);
}
