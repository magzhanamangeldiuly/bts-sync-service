package kz.tele2.bts.radio.model;

public record Site(
        String name,
        String bsc,
        String rnc,
        String state,
        String operator,
        String source,
        Double longitude,
        Double latitude
) {
}

