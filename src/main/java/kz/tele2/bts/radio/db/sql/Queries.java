package kz.tele2.bts.radio.db.sql;

public final class Queries {

    private Queries() {
    }

    public static final String SITES_ATOLL_DATA = """
            select upper(name) as name, 
                   longitude, 
                   latitude, 
                   kato, 
                   concat_ws(', ', region, district, city, address) as address
            from sites
            """;

    public static final String SITES_RADIO_DATA = """
            with rda as (select distinct site_name, bsc_rnc_name, testing
                         from radio_data_all
                         where insert_date = (select max(insert_date) from radio_data_all)
                           and site_name is not null),
                 rdaf as (select rda.site_name                                                      as name,
                                 max(rda.bsc_rnc_name) filter ( where left(bsc_rnc_name, 1) = 'B' ) as bsc,
                                 max(rda.bsc_rnc_name) filter ( where left(bsc_rnc_name, 1) = 'R' ) as rnc,
                                 'MTS'                                                              as operator,
                                 rda.testing
                          from rda
                          group by name, testing)
            select distinct upper(name) as name, bsc, rnc, operator,
                                      case testing
                                          when 'yes' then 1
                                          when 'no' then 0
                                          end as is_test,
                                      cast(null as double precision) as longitude,
                                      cast(null as double precision) as latitude,
                                      cast(null as varchar) as kato,
                                      cast(null as varchar) as address,
                                      'RADIO_DATA' as source,
                                      'main' as type
            from rdaf
            """;

    public static final String SITES_MOCN = """
            select distinct upper(c.site_number) as name,
                            cast(null as varchar) as bsc,
                            cast(null as varchar) as rnc,
                            'KCELL' as operator,
                            cast(null as integer) as is_test,
                            cast(null as double precision) as longitude,
                            cast(null as double precision) as latitude,
                            cast(null as varchar) as kato,
                            cast(null as varchar) as address,
                            'KCELL_CM' as source,
                            'main' as type
            from kcell_cm c
            where c.insert_date = (select max(insert_date) from kcell_cm)
              and site_number is not null
            """;

    public static final String SITES_250_PLUS = """
            select distinct upper(case
                                when operator = 'Kcell'
                                    then substring(nodeb_name, '\\d{5}')
                                else nodeb_name
                                end) as name,
                            cast(null as varchar) as bsc,
                            cast(null as varchar) as rnc,
                            upper(operator) as operator,
                            cast(null as integer) as is_test,
                            longitude,
                            latitude,
                            cast(null as varchar) as kato,
                            cast(null as varchar) as address,
                            '250_PLUS' as source,
                            'main' as type
            from beeline_kcell_250_plus bkp
            where nodeb_name is not null
            """;

    public static final String TRANSPORT_SITES = """
            select upper(SUBSTRING(REPLACE(REPLACE(site_name, '5G_', ''), 'A-', ''), 0, 7)) as name,
                   latitude,
                   longitude,
                   cast(null as varchar) as bsc,
                   cast(null as varchar) as rnc,
                   cast(null as varchar) as operator,
                   cast(null as integer) as is_test,
                   cast(null as varchar) as kato,
                   cast(null as varchar) as address,
                   'TRANSPORT' as source,
                   'transport' as type
            from basestation
            where latitude is not null
              and longitude is not null
            """;

    public static final String ROLLOUT_SITES = """
            SELECT upper(s.s_new_site_id) as name,
                   s.s_lat as latitude,
                   s.s_lng as longitude,
                   cast(null as varchar) as bsc,
                   cast(null as varchar) as rnc,
                   cast(null as varchar) as operator,
                   cast(null as integer) as is_test,
                   cast(null as varchar) as kato,
                   cast(null as varchar) as address,
                   'ROLLOUT' as source,
                   'rollout' as type
            FROM Sites s
            LEFT JOIN works w ON w.id = s.rolloutWork
            LEFT JOIN work_integrations t ON t.work = w.id
            LEFT JOIN (SELECT wilogs.work_integration_id,
                              MIN(wil.created_at) AS noc_finish
                       FROM work_integrations__logs wilogs
                       JOIN work_integration_logs wil ON wil.id = wilogs.`work-integration-log_id`
                       WHERE wil.action = 'finish'
                         AND wil.actor = 'noc'
                       GROUP BY wilogs.work_integration_id) logs ON logs.work_integration_id = t.id
            WHERE w.isRollout = 1
              and w.plannedYear = YEAR(CURDATE())
              AND s.s_noc_first_conf is null
            LIMIT 999999
            """;

    public static final String CELLS = """
            with rk as (select site_name                                                                 as site,
                               cell_name                                                                 as cell,
                               coalesce(
                                       regexp_substr(
                                               cell_id, '\\(([^()]*)\\)', 1, 1, 'i', 1), cell_id)::numeric as cellId,
                               coalesce(
                                       regexp_substr(
                                               lac_tac, '\\(([^()]*)\\)', 1, 1, 'i', 1), lac_tac)::numeric as lac,
                               upper(technology)                                                         as type,
                               case
                                   when technology = 'gsm' then
                                       case
                                           when arfcn_bcch between 512 and 885 then '1800'
                                           when arfcn_bcch between 128 and 251 then '850'
                                           when arfcn_bcch between 975 and 1023 or arfcn_bcch between 1 and 124 then '900'
                                           end
                                   when technology = 'umts' then
                                       case
                                           when arfcn_bcch between 10562 and 10838 then '2100'
                                           when arfcn_bcch between 4357 and 4458 then '850'
                                           when arfcn_bcch between 2937 and 3088 then '900'
                                           end
                                   when technology = 'lte' then
                                       case
                                           when arfcn_bcch between 0 and 599 then '2100'
                                           when arfcn_bcch between 1200 and 1949 then '1800'
                                           when arfcn_bcch between 6150 and 6449 then '800'
                                           when arfcn_bcch between 9210 and 9659 then '700'
                                           when arfcn_bcch between 3450 and 3799 then '900'
                                           when arfcn_bcch between 6780 and 7179 then '850'
                                           when arfcn_bcch between 2750 and 3449 then '2600'
                                           end
                                   when technology = 'nr' then
                                       case
                                           when arfcn_bcch = 432000 then '2100'
                                           when arfcn_bcch between 151600 and 160599 then '700'
                                           when arfcn_bcch between 643334 and 650000 then 'C-band -n78'
                                           end
                                   end as band,
                               status                                                                    as status
                        from radio_data_all
                        where insert_date = (select max(insert_date) from radio_data_all)
                        union all
                        select site_number as site,
                               cell_name as cell,
                               cell_id as cellId,
                               enodebid_gnbid as lac,
                               upper(technology) as type,
                               case
                                   when technology = 'gsm' then
                                       case
                                           when arfcn_dl between 512 and 885 then '1800'
                                           when arfcn_dl between 128 and 251 then '850'
                                           when arfcn_dl between 975 and 1023 or arfcn_dl between 1 and 124 then '900'
                                           end
                                   when technology = 'umts' then
                                       case
                                           when arfcn_dl between 10562 and 10838 then '2100'
                                           when arfcn_dl between 4357 and 4458 then '850'
                                           when arfcn_dl between 2937 and 3088 then '900'
                                           end
                                   when technology = 'lte' then
                                       case
                                           when arfcn_dl between 0 and 599 then '2100'
                                           when arfcn_dl between 1200 and 1949 then '1800'
                                           when arfcn_dl between 6150 and 6449 then '800'
                                           when arfcn_dl between 9210 and 9659 then '700'
                                           when arfcn_dl between 3450 and 3799 then '900'
                                           when arfcn_dl between 6780 and 7179 then '850'
                                           when arfcn_dl between 2750 and 3449 then '2600'
                                           end
                                   when technology = 'nr' then
                                       case
                                           when arfcn_dl = 432000 then '2100'
                                           when arfcn_dl between 151600 and 160599 then '700'
                                           when arfcn_dl between 643334 and 650000 then 'C-band -n78'
                                           end
                                   end as band,
                               case status
                                   when 'ACTIVE' then 'ACTIVATED'
                                   when 'UNLOCKED' then 'ACTIVATED'
                                   when 'INACTIVE' then 'DEACTIVATED'
                                   else status
                                   end as status
                        from kcell_cm
                        where insert_date = (select max(insert_date) from kcell_cm)),
                 rku as (select rk.*,
                                coalesce(
                                        nullif(substring(cell, position('-' in cell) - 3, 1), ''),
                                        nullif(substring(cell, length(cell) - position('_' in reverse(cell)) + 2, 1), ''),
                                        nullif(substring(cell, position('-' in cell) + 7, 1), '')
                                ) as sector
                         from rk where site is not null),
                 b as (select case substr(cell_name, length(cell_name) - 1, 1)
                                  when '1' then 'A'
                                  when '2' then 'B'
                                  when '3' then 'C'
                                  when '4' then 'D'
                                  else substr(cell_name, 6, 1)
                                  end as sector,
                              *
                       from beeline_kcell_250_plus
                       where nodeb_name is not null),
                 brku as (select site,
                                 cell,
                                 case sector
                                     when 'Z' then 'A'
                                     when 'Y' then 'B'
                                     when 'X' then 'C'
                                     when 'W' then 'D'
                                     else sector
                                     end as sector,
                                 cellid,
                                 lac,
                                 type,
                                 status,
                                 band
                          from rku
                          union
                          select case
                                     when operator = 'Kcell'
                                         then substring(nodeb_name, '\\d{5}')
                                     else nodeb_name
                                     end                     as site,
                                 cell_name                   as cell,
                                 sector,
                                 cell_id                     as cellId,
                                 lac,
                                 case technology
                                     when '3g' then 'UMTS'
                                     when '2g' then 'GSM'
                                     when '4g' then 'LTE'
                                     when '5g' then 'NR' end as type,
                                 'ACTIVATED'                 as status,
                                 null as band
                          from b
                 )
            select distinct on (cell) upper(cell) as cell, upper(site) as site, upper(sector) as sector, cellId, lac, type, status, band
            from brku
            where sector is not null
              and cellId is not null
              and lac is not null
              and type is not null
              and status is not null
            """;

    public static final String AZIMUTH_AND_HEIGHT = """
            with w as (select site_name as site, tx_id as cell, azimut, height, modified_date
                       from xgtransmitters
                       where active = 1
                       union all
                       select site_name as site, tx_id as cell, azimut, height, modified_date
                       from utransmitters
                       where active = 1
                       union all
                       select site_name as site, tx_id as cell, azimut, height, modified_date
                       from gtransmitters
                       where active = 1),
                 a as (select coalesce(
                                      nullif(substring(cell, charindex('-', cell, 3) - 3, 1), ''),
                                      nullif(substring(cell, len(cell) - charindex('_', reverse(cell)) + 2, 1), ''),
                                      nullif(substring(cell, charindex('-', cell) + 7, 1), '')
                              ) as sector,
                              site,
                              cell,
                              azimut,
                              height,
                              modified_date
                       from w),
                 b as (select iif(site like 'ERBS%' or site like 'GRBS%', substring(site, 6, 5), site) as site,
                              case
                                  when sector in ('Z', '1') then 'A'
                                  when sector in ('Y', '2') then 'B'
                                  when sector in ('X', '3') then 'C'
                                  when sector in ('W', '4') then 'D'
                                  else sector
                                  end as sector,
                              azimut,
                              height,
                              modified_date
                       from a
                       where sector is not null),
                 c as (select site,
                              sector,
                              azimut,
                              height,
                              row_number() over (partition by site, sector order by modified_date desc) as rn
                       from b)
            select upper(site) as site,
                   upper(sector) as sector,
                   azimut,
                   height
            from c
            where rn = 1
            """;

    public static final String CELL_INTERFERENCE = """
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

    public static final String SITE_WORKS = """
            SELECT upper(w.site_id) as site, t.name AS work_type, w.status
            FROM works AS w
            JOIN work_types AS t ON t.id = w.type
            WHERE w.isRollout = 0
              AND w.plannedYear = YEAR(CURDATE())
              AND w.isFlm = 0
            """;
}
