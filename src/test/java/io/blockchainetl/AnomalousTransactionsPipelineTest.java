package io.blockchainetl;

import io.blockchainetl.anomaloustransactions.BitcoinPipeline;
import io.blockchainetl.anomaloustransactions.EthereumPipeline;
import io.blockchainetl.anomaloustransactions.TestUtils;
import io.blockchainetl.anomaloustransactions.service.BigQueryServiceHolder;
import io.blockchainetl.anomaloustransactions.service.BigQueryServiceMock;
import io.blockchainetl.anomaloustransactions.utils.DataflowUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;


@RunWith(JUnit4.class)
public class AnomalousTransactionsPipelineTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void init() {
        BigQueryServiceHolder.INSTANCE = new BigQueryServiceMock();
        DataflowUtils.GENERATE_SEQUENCE_FOR_TESTING = true;
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testEthereumPipeline() throws Exception {
        testEthereumTemplate(
            "testdata/ethereumBlock1000000Transactions.json",
            "testdata/ethereumBlock1000000TransactionsExpected.json"
        );
    }

    @Test
    @Category(ValidatesRunner.class)
    public void testBitcoinPipeline() throws Exception {
        testBitcoinTemplate(
            "testdata/bitcoinBlock600340Transactions.json",
            "testdata/bitcoinBlock600340TransactionsExpected.json"
        );
    }

    private void testEthereumTemplate(String inputFile, String outputFile) throws IOException {
        List<String> blockchainData = TestUtils.readLines(inputFile);
        PCollection<String> input = p.apply("Input", Create.of(blockchainData));

        PCollection<String> output = EthereumPipeline.buildEthereumPipeline(p, input);

        TestUtils.logPCollection(output);

        PAssert.that(output).containsInAnyOrder(TestUtils.readLines(outputFile));

        p.run().waitUntilFinish();
    }

    private void testBitcoinTemplate(String inputFile, String outputFile) throws IOException {
        List<String> blockchainData = TestUtils.readLines(inputFile);
        PCollection<String> input = p.apply("Input", Create.of(blockchainData));

        PCollection<String> output = BitcoinPipeline.buildBitcoinPipeline(p, input);

        TestUtils.logPCollection(output);

        PAssert.that(output).containsInAnyOrder(TestUtils.readLines(outputFile));

        p.run().waitUntilFinish();
    }
}
