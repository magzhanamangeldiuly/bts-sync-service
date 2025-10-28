package kz.tele2.bts.radio.util;

import kz.tele2.bts.radio.model.AtollCellData;
import kz.tele2.bts.radio.model.CellData;
import kz.tele2.bts.radio.model.SiteData;

import java.util.Map;
public final class DataMapper {

    private DataMapper() {
    }

    public static SiteData toSiteData(Map<String, Object> row) {
        return new SiteData(
            getString(row, "name"),
            getString(row, "rnc"),
            getString(row, "bsc"),
            getDouble(row, "latitude"),
            getDouble(row, "longitude"),
            getString(row, "operator"),
            getString(row, "kato"),
            getInteger(row, "is_test"),
            getString(row, "address"),
            getString(row, "source")
        );
    }

    public static CellData toCellData(Map<String, Object> row) {
        return new CellData(
            getString(row, "cell"),
            getString(row, "site"),
            getString(row, "sector"),
            getLong(row, "cellid"),
            getInteger(row, "lac"),
            getString(row, "type"),
            getString(row, "status"),
            getString(row, "band"),
            getDouble(row, "azimut"),
            getDouble(row, "height")
        );
    }

    public static AtollCellData toAtollCellData(Map<String, Object> row) {
        return new AtollCellData(
            getString(row, "site"),
            getString(row, "sector"),
            getDouble(row, "azimut"),
            getDouble(row, "height")
        );
    }

    private static String getString(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value != null ? value.toString() : null;
    }

    private static Double getDouble(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Integer getInteger(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long getLong(Map<String, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

