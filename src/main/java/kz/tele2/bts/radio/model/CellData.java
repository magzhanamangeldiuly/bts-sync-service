package kz.tele2.bts.radio.model;

public record CellData(
    String cell,
    String site,
    String sector,
    Long cellid,
    Integer lac,
    String type,
    String status,
    String band,
    Double azimut,
    Double height
) {
    public CellData enrichWithAtoll(AtollCellData atollData) {
        if (atollData == null) {
            return this;
        }
        return new CellData(
            this.cell,
            this.site,
            this.sector,
            this.cellid,
            this.lac,
            this.type,
            this.status,
            this.band,
            atollData.azimut() != null ? atollData.azimut() : this.azimut,
            atollData.height() != null ? atollData.height() : this.height
        );
    }
    public String toAtollKey() {
        String siteUpper = site != null ? site.toUpperCase() : "";
        String sectorUpper = sector != null ? sector.toUpperCase() : "";
        return siteUpper + "-" + sectorUpper;
    }
}

