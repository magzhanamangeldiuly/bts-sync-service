package kz.tele2.bts.radio.sql;

public final class Oracle {
    public static final String UPSERT_SITES = """
                INSERT INTO OB_SITES_V2 (site, rnc, bsc, latitude, longitude, operator, kato, is_test, address, source, insert_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;


    public static final String DELETE_SITES = "delete OB_SITES_V2 where INSERT_DATE < (select max(INSERT_DATE) from OB_SITES_V2)";

    public static final String UPSERT_CELLS = """
                INSERT INTO OB_CELLS_V2 (cell, site, sector, cellid, lac, type, status, band, azimut, height, insert_date)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    public static final String DELETE_CELLS = "delete OB_CELLS_V2 where INSERT_DATE < (select max(INSERT_DATE) from OB_CELLS_V2)";
}