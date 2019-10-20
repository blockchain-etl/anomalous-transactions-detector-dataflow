package io.blockchainetl.anomaloustransactions.fns;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogElementsFn<E> extends ErrorHandlingDoFn<E, E> {
    private static final Logger LOG = LoggerFactory.getLogger(LogElementsFn.class);

    private String logPrefix = "";

    public LogElementsFn(String logPrefix) {
        this.logPrefix = logPrefix;
    }

    @Override
    public void doProcessElement(ProcessContext c) throws Exception {
        E element = c.element();
        LOG.info(logPrefix + element.toString());
        c.output(element);
    }
}
