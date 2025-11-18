package kz.tele2.bts.radio.db.sql;

public class Atoll {

    public final static String ATOLL_SITES = """
            with source
                     as (select case
                                    when patindex('%[0-9][0-9][0-9][0-9][0-9]%', upper(name)) > 0 then
                                        substring(upper(name), patindex('%[0-9][0-9][0-9][0-9][0-9]%', upper(name)), 5)
                                    when patindex('%[A-Z][A-Z][0-9][0-9][0-9][0-9]%', upper(name)) > 0 then
                                        substring(upper(name), patindex('%[A-Z][A-Z][0-9][0-9][0-9][0-9]%', upper(name)), 6)
                                    else
                                        upper(name)
                    end as name
                              , longitude
                              , latitude
                              , kato
                              , concat_ws(', ', region, district, city, address) as address
                         from sites)
            select name, max(longitude) as longitude, max(latitude) as latitude, max(kato) as kato, max(address) as address
            from source
            group by name
            """;

    public final static String ATOLL_TR_SITES = """
            select
                SUBSTRING(REPLACE(REPLACE(site_name, '5G_', ''), 'A-', ''), 0, 7) as name,
                latitude,
                longitude,
                'MTS' as operator,
                'TRANSPORT' as source
            from basestation where latitude is not null and longitude is not null
            """;

    public final static String ATOLL_CELLS = """
            with w (site, cell, azimuth, height, modified_date) as
                     (select site_name, tx_id, azimut, height, modified_date
                      from xgtransmitters
                      where active = 1
                      union all
                      select site_name, tx_id, azimut, height, modified_date
                      from utransmitters
                      where active = 1
                      union all
                      select site_name, tx_id, azimut, height, modified_date
                      from gtransmitters
                      where active = 1)
                    ,
                 a as
                     (select coalesce(
                                     nullif(substring(cell, charindex('-', cell, 3) - 3, 1), ''),
                                     nullif(substring(cell, len(cell) - charindex('_', reverse(cell)) + 2, 1), ''),
                                     nullif(substring(cell, charindex('-', cell) + 7, 1), '')
                             ) as sector,
                             site,
                             cell,
                             azimuth,
                             height,
                             modified_date
                      from w)
                    ,
                 b as
                     (select iif(site like 'ERBS%' or site like 'GRBS%', substring(site, 6, 5), site) as site,
                             case sector
                                 when 'Z' then 'A'
                                 when 'Y' then 'B'
                                 when 'X' then 'C'
                                 when 'W' then 'D'
                                 else sector
                                 end                                                                  as sector,
                             azimuth                                                                  as azimuth,
                             height                                                                   as height,
                             modified_date
                      from a
                      where sector is not null),
                 c as (SELECT site,
                              sector,
                              azimuth,
                              height,
                              ROW_NUMBER() OVER (PARTITION BY site, sector ORDER BY modified_date DESC) AS rn
                       FROM b),
                result as (select upper(concat(site, ':', sector)) as site, azimuth, height from c where rn = 1)
            select site as "key", max(azimuth) as azimuth, max(height) as height from result group by site
            """;
}
