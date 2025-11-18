package kz.tele2.bts.radio.transformers;

import kz.tele2.bts.radio.db.sql.Postgis;
import kz.tele2.bts.radio.model.Site;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
public class NonExsistInPostgisTransformer extends AbstractTransformer {
    private final JdbcTemplate postgisJdbcTemplate;

    public NonExsistInPostgisTransformer(JdbcTemplate postgisJdbcTemplate) {
        this.postgisJdbcTemplate = postgisJdbcTemplate;
    }

    @Override
    protected Object doTransform(Message<?> message) {
        var objects = (List<Site>) message.getPayload();
        List<String> siteNames = postgisJdbcTemplate.queryForList(Postgis.SITE_NAMES, String.class);
        Set<String> siteNameSet = new HashSet<>(siteNames);
        return objects.stream().filter(site -> !siteNameSet.contains(site.getName())).toList();
    }
}
