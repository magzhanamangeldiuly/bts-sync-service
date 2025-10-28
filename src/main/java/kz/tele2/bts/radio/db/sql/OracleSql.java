package kz.tele2.bts.radio.db.sql;

public class OracleSql {
    public final static String SITES_UPSERT = """
            merge into ob_sites tgt
                using (select ? as site, ? as rnc, ? as bsc, ? as latitude, ? as longitude, ? as operator, ? as kato, ? as is_test, ? as address, ? as source FROM dual) src
                on (tgt.site = src.site)
                when matched then
                    update set tgt.rnc = src.rnc,
                               tgt.bsc = src.bsc,
                               tgt.latitude = src.latitude,
                               tgt.longitude = src.longitude,
                               tgt.operator = src.operator,
                               tgt.kato = src.kato,
                               tgt.is_test = src.is_test,
                               tgt.address = src.address,
                               tgt.source = src.source
                when not matched then
                    insert (site, rnc, bsc, latitude, longitude, operator, kato, is_test, address, source)
                    values (src.site, src.rnc, src.bsc, src.latitude, src.longitude, src.operator, src.kato, src.is_test, src.address, src.source)
        """;

    public final static String DELETE_CELL_INTERFERENCE_BY_SITES = "delete from ob_cell_interference where cell in (select cell from ob_cells where site in (:sites))";
    public final static String DELETE_SITE_AZIMUTH_BY_SITES = "delete from ob_site_azimuth where site in (:sites)";
    public final static String DELETE_CELL_BY_SITES = "delete from ob_cells where site in (:sites)";
    public final static String DELETE_SITES = "delete from ob_sites where site in (:sites)";
    public final static String DELETE_SITES_RO = "delete from ob_rollout_sites where site in (:sites)";
    public final static String DELETE_SITES_TR = "delete from ob_transport_sites where site in (:sites)";

    public final static String CELLS_UPSERT = """
            merge into ob_cells tgt
            using (select ? as site, ? as cell, ? as cellid, ? as sector, ? as lac, ? as type, ? as status, ? as band from dual) src
            on (tgt.cell = src.cell)
            when matched then
                update set tgt.site = src.site,
                           tgt.cellid = src.cellid,
                           tgt.sector = src.sector,
                           tgt.lac = src.lac,
                           tgt.type = src.type,
                           tgt.status = src.status,
                           tgt.band = src.band
            when not matched then
                insert (site, cell, cellid, sector, lac, type, status, band)
                values (src.site, src.cell, src.cellid, src.sector, src.lac, src.type, src.status, src.band)
            """;

    public final static String DELETE_INTERFERENCE_BY_CELLS = "delete from ob_cell_interference where cell in (:cells)";
    public final static String DELETE_CELLS = "delete from ob_cells where cell in (:cells)";

    public static String CELLS_INTERFERENCE_UPSERT = """
            merge into ob_cell_interference tgt
            using (select ? as cell, ? as value from dual) src
            on (tgt.cell = src.cell)
            when matched then
                update set tgt.value = src.value
            when not matched then
                insert (cell, value)
                values (src.cell, src.value)
            """;

    public final static String DELETE_CELL_INTERFERENCES = "delete from ob_cell_interference where cell in (:payload)";

    public final static String SITE_AZIMUTH_UPSERT = """
            merge into ob_site_azimuth tgt
            using (select ? as site, ? as sector, ? as value from dual) src
            on (tgt.site = src.site and tgt.sector = src.sector)
            when matched then
                update set tgt.value = src.value    
            when not matched then
                insert (site, sector, value)
                values (src.site, src.sector, src.value)
            """;

    public final static String SITE_AZIMUTH_DELETE= "delete from ob_site_azimuth where site = ? and sector = ?";
    public final static String CELL_NAMES = "select distinct cell from ob_cells";
    public final static String SITES = "select distinct site from ob_sites";
    public final static String SITES_MTS = "select distinct site from ob_sites where operator = 'MTS'";
    public final static String TR_SITE_NAMES = "select distinct site from ob_transport_sites";
    public final static String SITES_RO_UPSERT = """
        merge into ob_rollout_sites tgt
        using (select ? as site, ? as latitude, ? as longitude from dual) src
        on (tgt.site = src.site)
        when matched then
            update set tgt.latitude = src.latitude,
                       tgt.longitude = src.longitude
        when not matched then
            insert (site, latitude, longitude)
            values (src.site, src.latitude, src.longitude)
        """;
    public final static String SITES_TR_UPSERT = """
        merge into ob_transport_sites tgt
        using (select ? as site, ? as latitude, ? as longitude from dual) src
        on (tgt.site = src.site)
        when matched then
            update set tgt.latitude = src.latitude,
                       tgt.longitude = src.longitude
        when not matched then
            insert (site, latitude, longitude)
            values (src.site, src.latitude, src.longitude)
        """;

    public final static String SECTOR_HEIGHT_UPSERT = """
            merge into ob_site_height tgt
            using (select ? as site, ? as sector, ? as value from dual) src
            on (tgt.site = src.site and tgt.sector = src.sector)
            when matched then
                update set tgt.value = src.value    
            when not matched then
                insert (site, sector, value)
                values (src.site, src.sector, src.value)
            """;
    public final static String SECTOR_HEIGHT_DELETE = "delete from ob_site_height where site = ? and sector = ?";
    public final static String DELETE_SECTOR_HEIGHT_BY_SITES = "delete from ob_site_height where site in (:sites)";

    public final static String DELETE_SITES_WORKS = "delete from ob_site_works where site in (:sites)";
    public final static String SITE_WORKS_UPSERT = """
        merge into ob_site_works tgt
        using (select ? as site, ? as work_type, ? as status from dual) src
        on (tgt.site = src.site)
        when matched then
            update set tgt.work_type = src.work_type,
                       tgt.status = src.status
        when not matched then
            insert (site, work_type, status)
            values (src.site, src.work_type, src.status)
        """;
}
