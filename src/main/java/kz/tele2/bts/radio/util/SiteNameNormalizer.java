package kz.tele2.bts.radio.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class SiteNameNormalizer {

    private static final Pattern ERBS_PATTERN = Pattern.compile("^ERBS_(\\d+)");
    private static final Pattern LEADING_DIGITS = Pattern.compile("^(\\d+)");

    private SiteNameNormalizer() {
    }

    public static String normalize(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }

        if (name.startsWith("ERBS_")) {
            Matcher matcher = ERBS_PATTERN.matcher(name);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }

        char firstChar = name.charAt(0);

        if (Character.isDigit(firstChar)) {
            Matcher matcher = LEADING_DIGITS.matcher(name);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }

        int firstUnderscore = name.indexOf('_');

        if (firstUnderscore == -1) {
            return name;
        }

        String firstSegment = name.substring(0, firstUnderscore);

        if (containsDigit(firstSegment)) {
            return firstSegment;
        }

        int secondUnderscore = name.indexOf('_', firstUnderscore + 1);

        if (secondUnderscore > 0) {
            return name.substring(0, secondUnderscore);
        }
        return name;
    }

    private static boolean containsDigit(String str) {
        int len = str.length();
        for (int i = 0; i < len; i++) {
            if (Character.isDigit(str.charAt(i))) {
                return true;
            }
        }
        return false;
    }
}

