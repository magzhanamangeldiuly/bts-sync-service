package kz.tele2.bts.radio.utils;

import com.google.common.collect.Sets;
import org.springframework.integration.store.MessageGroup;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Utils {
    public final static String NULL_CHANNEL = "nullChannel";
    private static final Pattern FIVE_DIGITS = Pattern.compile("\\d{5}");
    private static final Pattern TWO_LETTERS_FOUR_DIGITS = Pattern.compile("[A-Z]{2}\\d{4}");

    @SuppressWarnings("unchecked")
    public static Map<String, Map<String, Object>> convertToMapByProperty(List<?> list, String property) {
        return list.stream()
                .filter(el -> ((Map<String, Object>) el).get(property) != null)
                .collect(
                Collectors.toMap(
                        k -> (String) ((Map<String, Object>) k).get(property),
                        v -> (Map<String, Object>) v)
        );
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Map<String, Object>> convertToMapByPropertyV2(List<?> list, String property) {
        return list.stream()
                .filter(el -> ((Map<String, Object>) el).get(property) != null)
                .collect(
                Collectors.toMap(
                        k -> normalizeSiteName((String) ((Map<String, Object>) k).get(property)),
                        v -> (Map<String, Object>) v)
        );
    }


    public static String normalizeSiteName(String name) {
        if (name == null || name.isBlank()) {
            return null;
        }
        String upper = name.toUpperCase();
        Matcher m1 = FIVE_DIGITS.matcher(upper);
        if (m1.find()) {
            return m1.group();
        }
        Matcher m2 = TWO_LETTERS_FOUR_DIGITS.matcher(upper);
        if (m2.find()) {
            return m2.group();
        }
        return upper;
    }

    public static Optional<Message<?>> extractMessageByHeader(MessageGroup group, String header, String headerValue) {
        return group.getMessages()
                .stream()
                .filter(s -> Objects.equals(s.getHeaders().get(header), headerValue))
                .findFirst();
    }



    @SuppressWarnings("unchecked")
    public static Message<?> sourceTargetDifference(MessageGroup group) {
        var msg = group.getOne();

        var targetMsg = extractMessageByHeader(group, "data", "target");
        var target = targetMsg.map(message -> new HashSet<>((List<String>) message.getPayload())).orElseGet(HashSet::new);

        var sourceMsg = extractMessageByHeader(group, "data", "source");
        var source = sourceMsg.map(message -> new HashSet<>((List<String>) message.getPayload())).orElseGet(HashSet::new);

        var diff = Sets.difference(source, target);
        return MessageBuilder.withPayload(diff)
                .copyHeaders(msg.getHeaders())
                .build();
    }

    public static Object coalesce(Object o1, Object o2) {
        return o1 == null ? o2 : o1;
    }
}
