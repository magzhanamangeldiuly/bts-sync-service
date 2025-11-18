package kz.tele2.bts.radio.db.sql;

public final class RanSharing {

    public static final String SITES = """
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
                                   group by name, testing),
                          f as (select *,
                                       cast(null as double precision) as longitude,
                                       cast(null as double precision) as latitude,
                                       'RADIO_DATA'                   as source
                                from rdaf
                                union all
                                select distinct c.site_number                  as name,
                                                null,
                                                null,
                                                'KCELL',
                                                null,
                                                cast(null as double precision) as longitude,
                                                cast(null as double precision) as latitude,
                                                'KCELL_CM'                     as source
                                from kcell_cm c
                                where c.insert_date = (select max(insert_date) from kcell_cm)
                                  and site_number is not null
                                union all
                                select distinct case
                                                    when operator = 'Kcell'
                                                        then substring(nodeb_name, '\\d{5}')
                                                    else nodeb_name
                                                    end         as name,
                                                null,
                                                null,
                                                upper(operator) as operator,
                                                null,
                                                longitude,
                                                latitude,
                                                '250_PLUS'      as source
                                from beeline_kcell_250_plus bkp
                                where nodeb_name is not null)
                     select distinct on (name) upper(name) as name,
                                               bsc,
                                               rnc,
                                               operator,
                                               case testing
                                                   when 'yes' then 1
                                                   when 'no' then 0
                                                   end     as "isTest",
                                               longitude,
                                               latitude,
                                               source
                     from f
            """;

    public final static String CELLS = """
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
            select 
                distinct on (cell) upper(cell) as cell, 
                upper(site) as site, 
                upper(sector) as sector, 
                cellId, 
                lac, 
                type, 
                status, 
                band
            from brku
            where sector is not null
              and cellId is not null
              and lac is not null
              and type is not null
              and status is not null
            """;
}
