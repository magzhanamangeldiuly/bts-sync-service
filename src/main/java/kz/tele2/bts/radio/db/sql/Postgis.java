package kz.tele2.bts.radio.db.sql;

public class Postgis {
    public final static String DATASTORE_SITE_REGION = "SITE";
    public final static String DATASTORE_CELL_REGION = "CELL";
    public final static String DATASTORE_INTERFERENCE_REGION = "INTERFERENCE";

    public final static String INSERT_DATA_STORE = """
            INSERT INTO data_store (key, region, value)
            VALUES (?, ?, ?::jsonb)
            ON CONFLICT (key, region)
            DO UPDATE SET value = EXCLUDED.value
            """;

    public final static String SELECT_DATASTORE_QUERY = """
            select value::varchar from data_store
            where region = :region
              and key in (:keys)
            """;

    public final static String SITE_NAMES = "select site from sites where insert_date = (select max(insert_date) from sites where source in ('250_PLUS', 'KCELL_CM', 'RADIO_DATA') and source in ('250_PLUS', 'KCELL_CM', 'RADIO_DATA'))";
}
