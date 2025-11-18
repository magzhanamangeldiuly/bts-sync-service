package kz.tele2.bts.radio.model;

import java.util.Map;

import static java.util.Objects.nonNull;
import static kz.tele2.bts.radio.utils.Utils.coalesce;

public record RanSharingCell(
        String cell,
        String site,
        String sector,
        Long cellId,
        Long lac,
        String type,
        String status,
        String band
) {
    public Cell appendAzimuthAndHeight(Map<String, Object> dataStoreMap) {
        Double h = null, a = null;
        if (nonNull(dataStoreMap)) {
            h = (Double) coalesce(dataStoreMap.get("height"), null);
            a = (Double) coalesce(dataStoreMap.get("azimuth"), null);
        }
        return new Cell(
                this.cell,
                this.site,
                this.sector,
                this.cellId,
                this.lac,
                this.type,
                this.status,
                this.band,
                h,
                a,
                null);
    }
}
