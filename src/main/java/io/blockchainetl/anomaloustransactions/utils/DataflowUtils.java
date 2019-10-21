package io.blockchainetl.anomaloustransactions.utils;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;

public class DataflowUtils {
    
    // Hack for testing. Prevents the tests being stalled because of infinite sequence.
    public static boolean GENERATE_SEQUENCE_FOR_TESTING = false;

    // https://beam.apache.org/documentation/patterns/side-input-patterns/#using-global-window-side-inputs-in-non
    // -global-windows
    public static <T> PCollectionView<T> getPCollectionViewForValue(Pipeline p, String prefix, DoFn<Long, T> queryValueFn) {
        GenerateSequence sequence = GenerateSequence.from(0).withRate(1, Duration.standardHours(24L));
        if (GENERATE_SEQUENCE_FOR_TESTING) {
            sequence = sequence.to(1);
        }
        PCollectionView<T> pCollectionView =
            p.apply(prefix + "GenerateSequence", sequence)
                .apply(prefix + "Window", Window.<Long>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes()
                )
                .apply(prefix + "Query", ParDo.of(queryValueFn))
                .apply(prefix + "Singleton", View.asSingleton());
        return pCollectionView;
    }
}
