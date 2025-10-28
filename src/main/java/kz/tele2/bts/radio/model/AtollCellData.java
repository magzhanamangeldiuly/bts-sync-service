package kz.tele2.bts.radio.model;

public record AtollCellData(
    String site,
    String sector,
    Double azimut,
    Double height
) {
    public String toNormalizedKey() {
        String normalizedSite = kz.tele2.bts.radio.util.SiteNameNormalizer.normalize(site);
        String siteUpper = normalizedSite != null ? normalizedSite.toUpperCase() : "";
        String sectorUpper = sector != null ? sector.toUpperCase() : "";
        return siteUpper + "-" + sectorUpper;
    }
}

