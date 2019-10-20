package io.blockchainetl.anomaloustransactions.fns;


import io.blockchainetl.anomaloustransactions.utils.JsonUtils;

public class EncodeToJsonFn extends ErrorHandlingDoFn<Object, String> {

    @Override
    protected void doProcessElement(ProcessContext c) throws Exception {
        Object elem = c.element();
        c.output(JsonUtils.encodeJson(elem));
    }
}
