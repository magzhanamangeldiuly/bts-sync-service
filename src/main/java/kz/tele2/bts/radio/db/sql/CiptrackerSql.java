package kz.tele2.bts.radio.db.sql;

public class CiptrackerSql {
    public final static String RO = """
        SELECT s.s_new_site_id as name,
                           s.s_lat         as latitude,
                           s.s_lng         as longitude
                    FROM Sites s
                             LEFT JOIN works w
                                       ON w.id = s.rolloutWork
                             LEFT JOIN work_integrations t
                                       ON t.work = w.id
                             LEFT JOIN (SELECT wilogs.work_integration_id,
                                               MIN(wil.created_at) AS noc_finish
                                        FROM work_integrations__logs wilogs
                                                 JOIN work_integration_logs wil
                                                      ON wil.id = wilogs.`work-integration-log_id`
                                        WHERE wil.action = 'finish'
                                          AND wil.actor = 'noc'
                                        GROUP BY wilogs.work_integration_id) logs
                                       ON logs.work_integration_id = t.id
                    WHERE w.isRollout = 1
                      and w.plannedYear = YEAR(CURDATE())
                      AND s.s_noc_first_conf is null
                    LIMIT 999999
            """;

    public final static String WORKS = """
        SELECT w.site_id as site, t.name AS work_type, w.status
        FROM works AS w
        JOIN work_types AS t ON t.id = w.type
        WHERE w.isRollout = 0
          AND w.plannedYear = YEAR(CURDATE())
          AND w.isFlm = 0
            """;

}
