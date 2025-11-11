package kz.tele2.bts.radio.db.sql;

public final class Queries {

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
}
