package io.blockchainetl;

import io.blockchainetl.anomaloustransactions.Constants;
import io.blockchainetl.anomaloustransactions.EthereumPipeline;
import io.blockchainetl.anomaloustransactions.TestUtils;
import io.blockchainetl.anomaloustransactions.service.BigQueryService;
import io.blockchainetl.anomaloustransactions.service.BigQueryServiceHolder;
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
import java.math.BigInteger;
import java.util.List;


@RunWith(JUnit4.class)
public class AnomalousTransactionsPipelineTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Before
    public void init() {
        BigQueryServiceHolder.INSTANCE = new BigQueryService() {
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
                return new BigInteger("100000000000");
            }
        };
    }
    
    @Test
    @Category(ValidatesRunner.class)
    public void testEthereumPipeline() throws Exception {
        testTemplate(
            "testdata/ethereumBlock1000000Transactions.json",
            "testdata/ethereumBlock1000000TransactionsExpected.json"
        );
    }
    
    private void testTemplate(String inputFile, String outputFile) throws IOException {
        List<String> blockchainData = TestUtils.readLines(inputFile);
        PCollection<String> input = p.apply("Input", Create.of(blockchainData));

        PCollection<String> output = EthereumPipeline.buildEthereumPipeline(p, input);

        TestUtils.logPCollection(output);

        PAssert.that(output).containsInAnyOrder(TestUtils.readLines(outputFile));

        p.run().waitUntilFinish();  
    }
}
