/**
 * 
 */
package com.cachakka.streaming.core.utils;

import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Random;
import java.util.TimeZone;

public class Utils {

    // The following fields help maintain formats between different jobs
    public static String pattern = "yyyy-MM-dd HH:mm:ss.SSS Z";
    private static DateFormat DATE_FORMAT = new SimpleDateFormat(pattern);

    public static String hivePattern = "yyyy-MM-dd HH:mm:ss.SSSSSSZZ";

    public static String timeZoneId = "America/Los_Angeles";
    public static DateTimeZone TIME_ZONE = DateTimeZone.forTimeZone(TimeZone.getTimeZone(timeZoneId));

    static {
        DATE_FORMAT.setTimeZone(TIME_ZONE.toTimeZone());
    }

    /**
     * Convert an integer to a positive number
     * @param number integer number
     * @return positive integer number
     */
    public static long toPositive(int number) {
        return ((long) number) & 0xffffffffL;
    }

    public static DateTime currentTime(){
        return new DateTime(TIME_ZONE);
    }

    /**
     * Convert a timestamp string to Datetime Object
     */
    public static DateTime toDateTime(String timestamp){
        DateTimeFormatter formatter = DateTimeFormat.forPattern(hivePattern);
        return formatter.parseDateTime(timestamp);
    }

    public static DateFormat dateFormat() {
        return DATE_FORMAT;
    }


    /**
     * Generates 32 bit murmur hash from byte array
     * @param data byte array to hash
     * @return 32 bit hash of the given array
     */
    public static int murmur(final byte[] data) {
        return Hashing.murmur3_32().hashBytes(data).asInt();
    }

    /**
     * Given a Double, returns a Double which is rounded DOWN & to 2 decimal places
     *
     * @param d Double number to round
     * @return Rounded Double
     */
    public static Double getRoundedDouble(Double d) {
        if (d == null) {
            return d;
        } else {
			/*
			 * We assume rounding to 2 decimal places.
			 */
            DecimalFormat decimalFormat = new DecimalFormat("#.##");
            decimalFormat.setRoundingMode(RoundingMode.DOWN);
            return Double.valueOf(decimalFormat.format(d + 0.005));
        }
    }

    /**
     * Generates a random alpha-numeric string
     * @return
     */
    public static String generateRandomAlphanumeric(){
            Random random = new Random();
            final byte[] buffer = new byte[5];
            random.nextBytes(buffer);
            return BaseEncoding.base64Url().omitPadding().encode(buffer); // or base32()
    }
}
