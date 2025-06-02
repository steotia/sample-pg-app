// src/test/java/com/trials/crdb/app/test/TimeBasedTest.java
package com.trials.crdb.app.test;

import com.trials.crdb.app.utils.DateTimeProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public abstract class TimeBasedTest {
    
    protected ZonedDateTime baseTime;
    
    @BeforeEach
    public void setupTime() {
        // Set up a fixed date time for testing
        baseTime = ZonedDateTime.of(2026, 6, 15, 12, 0, 0, 0, ZoneId.systemDefault()
        ).truncatedTo(ChronoUnit.SECONDS);
        
        DateTimeProvider.useFixedClockAt(baseTime);
    }
    
    @AfterEach
    public void resetTime() {
        DateTimeProvider.useSystemClock();
    }
    
    protected void setTimeOffset(long days) {
        DateTimeProvider.useFixedClockAt(baseTime.plusDays(days));
    }
}