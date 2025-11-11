package kz.tele2.bts.radio.handler;

import kz.tele2.bts.radio.model.Site;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SitesUpsertHandler {

    private final NamedParameterJdbcTemplate postgisJdbcTemplate;

    public SitesUpsertHandler(@Qualifier("postgisNamedParameterJdbcTemplate") NamedParameterJdbcTemplate postgisJdbcTemplate) {
        this.postgisJdbcTemplate = postgisJdbcTemplate;
    }

    @Transactional(transactionManager = "postgisTransactionManager")
    public List<Site> handle(Message<List<Site>> message) {
        List<Site> sites = message.getPayload();
        Timestamp insertDate = (Timestamp) message.getHeaders().get("insert_date");

        if (sites == null || sites.isEmpty()) {
            log.info("No sites to upsert");
            return sites;
        }

        Set<String> operators = sites.stream()
                .map(Site::operator)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        if (!operators.isEmpty()) {
            log.info("Deleting existing sites for operators: {}", operators);
            MapSqlParameterSource deleteParams = new MapSqlParameterSource("operators", operators);
            int deleted = postgisJdbcTemplate.update(
                    "DELETE FROM sites WHERE operator IN (:operators)",
                    deleteParams
            );
            log.info("Deleted {} existing sites", deleted);
        }

        // 3. Фильтруем записи с null координатами (latitude и longitude - NOT NULL в БД)
        List<Site> validSites = sites.stream()
                .filter(site -> site.latitude() != null && site.longitude() != null)
                .toList();

        if (validSites.isEmpty()) {
            log.warn("All sites filtered out due to null coordinates");
            return sites;
        }

        if (validSites.size() < sites.size()) {
            log.warn("Filtered out {} sites with null coordinates", sites.size() - validSites.size());
        }

        // 4. Вставить новые записи (batch)
        String insertSql = """
            INSERT INTO sites (site, rnc, bsc, latitude, longitude, operator, is_test, source, insert_date)
            VALUES (:site, :rnc, :bsc, :latitude, :longitude, :operator, :isTest, :source, :insertDate)
            """;

        MapSqlParameterSource[] batchParams = validSites.stream()
                .map(site -> new MapSqlParameterSource()
                        .addValue("site", site.name())
                        .addValue("rnc", site.rnc())
                        .addValue("bsc", site.bsc())
                        .addValue("latitude", site.latitude())
                        .addValue("longitude", site.longitude())
                        .addValue("operator", site.operator())
                        .addValue("isTest", isTestState(site.state()))
                        .addValue("source", site.source())
                        .addValue("insertDate", insertDate))
                .toArray(MapSqlParameterSource[]::new);

        int[] results = postgisJdbcTemplate.batchUpdate(insertSql, batchParams);
        int inserted = 0;
        for (int result : results) {
            inserted += result;
        }

        log.info("Inserted {} new sites into PostGIS", inserted);
        return sites;
    }

    private boolean isTestState(String state) {
        return state != null && "TEST".equalsIgnoreCase(state.trim());
    }
}

