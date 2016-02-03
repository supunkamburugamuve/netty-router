package edu.soic.indiana.raava.router.netty;

public class Utils {
    public static Integer getInt(Object o) {
        Integer result = getInt(o, null);
        if (null == result) {
            throw new IllegalArgumentException("Don't know how to convert null to int");
        }
        return result;
    }

    public static Integer getInt(Object o, Integer defaultValue) {
        if (null == o) {
            return defaultValue;
        }

        if (o instanceof Number) {
            return ((Number) o).intValue();
        } else if (o instanceof String) {
            return Integer.parseInt(((String) o));
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " to int");
        }
    }

    public static void sleepMs(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignore) {
        }
    }
}
