package kz.tele2.bts.radio.model;

public record SiteData(
    String name,
    String rnc,
    String bsc,
    Double latitude,
    Double longitude,
    String operator,
    String kato,
    Integer isTest,
    String address,
    String source
) {
    public SiteData mergeWith(SiteData other) {
        if (other == null) {
            return this;
        }
        return new SiteData(
            this.name != null ? this.name : other.name,
            this.rnc != null ? this.rnc : other.rnc,
            this.bsc != null ? this.bsc : other.bsc,
            this.latitude != null ? this.latitude : other.latitude,
            this.longitude != null ? this.longitude : other.longitude,
            this.operator != null ? this.operator : other.operator,
            this.kato != null ? this.kato : other.kato,
            this.isTest != null ? this.isTest : other.isTest,
            this.address != null ? this.address : other.address,
            this.source != null ? this.source : other.source
        );
    }
    public SiteData withNormalizedKato() {
        String normalizedKato = (kato != null && kato.matches("\\d+")) ? kato : null;
        return new SiteData(name, rnc, bsc, latitude, longitude, operator, normalizedKato, isTest, address, source);
    }
}

