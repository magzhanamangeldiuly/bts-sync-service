package kz.tele2.bts.radio.model;

import kz.tele2.bts.radio.utils.Utils;
import org.springframework.validation.beanvalidation.MessageSourceResourceBundleLocator;

import java.util.Map;

import static kz.tele2.bts.radio.utils.Utils.coalesce;

public record Cell(
        String cell,
        String site,
        String sector,
        Long cellId,
        Long lac,
        String type,
        String status,
        String band,
        Double height,
        Double azimuth,
        Double interference
) {
    public Cell appendInterference(Map<String, Object> dataStoreMap) {
        Double i = null;
        if (dataStoreMap != null) {
            i = (Double) coalesce(dataStoreMap.get("value"), null);
        }
        if (cell.equals("UK7150A0700N1")){
            System.out.println(1);
        }
        return new Cell(
                cell,
                site,
                sector,
                cellId,
                lac,
                type,
                status,
                band,
                height,
                azimuth,
                i
        );
    }
}
