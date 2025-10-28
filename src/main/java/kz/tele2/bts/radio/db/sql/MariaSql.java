package kz.tele2.bts.radio.db.sql;


public class MariaSql {
    public final static String CELL_INTERFERENCE = """
        with w as (select cell, null as value
                   from interference_2G
                   union all
                   select *
                   from interference_3G
                   union all
                   select concat('A-', cell) as cell, i.avg_interference
                   from interference_4G i
                   union all
                   select cell, avg_interference as value
                   from interference_5G)
        select distinct upper(cell) as cell, value from w
        """;

    public final static String CELL_NAMES = """
        with w as (select cell as cell, null as value
                   from interference_2G
                   union all
                   select *
                   from interference_3G
                   union all
                   select concat('A-', cell) as cell, i.avg_interference
                   from interference_4G i
                   union all
                   select *
                   from interference_5G)
        select distinct upper(cell) as cell from w
        """;


}
