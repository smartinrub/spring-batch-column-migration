package com.sergiomartinrubio.springbatchcolumnmigration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;

@Slf4j
public class ItemVerificationSkipper implements SkipPolicy {
    @Override
    public boolean shouldSkip(final Throwable exception, final int skipCount) throws SkipLimitExceededException {
        final String errorMessage = "Entry not found on first table. Exception: "
            + exception.getMessage()
            + "\n";
        log.error("{}", errorMessage);
        return true;
    }
}
