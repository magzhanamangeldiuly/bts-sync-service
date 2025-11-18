package kz.tele2.bts.radio.model;

import java.util.Map;

public record DataStoreObject(
        String key,
        Map<String, Object> value
) {
}
