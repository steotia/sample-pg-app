// src/main/java/com/trials/crdb/app/utils/DateTimeProvider.java
package com.trials.crdb.app.utils;

import java.time.Clock;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class DateTimeProvider {
    private static Clock clock = Clock.systemDefaultZone();
    
    // Private constructor to prevent instantiation
    private DateTimeProvider() {}
    
    public static ZonedDateTime now() {
        return ZonedDateTime.now(clock);
    }
    
    public static void useFixedClockAt(ZonedDateTime dateTime) {
        clock = Clock.fixed(
            dateTime.toInstant(), 
            dateTime.getZone()
        );
    }
    
    public static void useSystemClock() {
        clock = Clock.systemDefaultZone();
    }
}