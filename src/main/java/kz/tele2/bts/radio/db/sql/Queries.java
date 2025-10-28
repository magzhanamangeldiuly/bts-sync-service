package kz.tele2.bts.radio.db.sql;

public class Queries {
    
    public static final String SITES_RADIO_DATA = """
        SELECT name, rnc, bsc, latitude, longitude, operator, kato, is_test, address, 'radio_data' as source, 'main' as type
        FROM radio_data_all
        WHERE name IS NOT NULL
        """;
    
    public static final String SITES_MOCN = """
        SELECT name, rnc, bsc, latitude, longitude, operator, kato, is_test, address, 'mocn' as source, 'mocn' as type
        FROM kcell_cm
        WHERE name IS NOT NULL
        """;
    
    public static final String SITES_250_PLUS = """
        SELECT name, rnc, bsc, latitude, longitude, operator, kato, is_test, address, '250_plus' as source, '250_plus' as type
        FROM beeline_kcell_250_plus
        WHERE name IS NOT NULL
        """;
    
    public static final String TRANSPORT_SITES = """
        SELECT name, latitude, longitude, 'transport' as source, 'transport' as type
        FROM transport_sites
        WHERE name IS NOT NULL
        """;
    
    public static final String ROLLOUT_SITES = """
        SELECT name, latitude, longitude, 'rollout' as source, 'rollout' as type
        FROM rollout_sites
        WHERE name IS NOT NULL
        """;
    
    public static final String CELLS = """
        SELECT cell, site, sector, cellid, lac, type, status, band
        FROM radio_data_all
        WHERE cell IS NOT NULL AND site IS NOT NULL
        """;
    
    public static final String AZIMUTH_AND_HEIGHT = """
        SELECT site, sector, azimut, height
        FROM (
            SELECT site, sector, azimut, height FROM xgtransmitters
            UNION ALL
            SELECT site, sector, azimut, height FROM utransmitters
            UNION ALL
            SELECT site, sector, azimut, height FROM gtransmitters
        ) t
        WHERE site IS NOT NULL AND sector IS NOT NULL
        """;
    
    public static final String CELL_INTERFERENCE = """
        SELECT cell, value
        FROM cell_interference
        WHERE cell IS NOT NULL
        """;
    
    public static final String SITE_WORKS = """
        SELECT site, work_type, status
        FROM site_works
        WHERE site IS NOT NULL
            """;
}