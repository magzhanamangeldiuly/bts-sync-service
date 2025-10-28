package kz.tele2.bts.radio.config;

import javax.sql.DataSource;
import kz.tele2.bts.radio.db.sql.Queries;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.jdbc.core.ColumnMapRowMapper;

@Slf4j
@Configuration
public class JdbcPollingChannelAdapterConfiguration {

    @Bean("sitesRadioDataAdapter")
    public JdbcPollingChannelAdapter sitesRadioDataAdapter(
        @Qualifier("postgresDataSource") DataSource postgresDataSource) {
        var adapter = new JdbcPollingChannelAdapter(postgresDataSource, Queries.SITES_RADIO_DATA);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }

    @Bean("sitesMocnAdapter")
    public JdbcPollingChannelAdapter sitesMocnAdapter(@Qualifier("postgresDataSource") DataSource postgresDataSource) {
        var adapter = new JdbcPollingChannelAdapter(postgresDataSource, Queries.SITES_MOCN);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }

    @Bean("sites250PlusAdapter")
    public JdbcPollingChannelAdapter sites250PlusAdapter(
        @Qualifier("postgresDataSource") DataSource postgresDataSource) {
        var adapter = new JdbcPollingChannelAdapter(postgresDataSource, Queries.SITES_250_PLUS);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }

    @Bean("transportSitesAdapter")
    public JdbcPollingChannelAdapter transportSitesAdapter(
        @Qualifier("atollTrDataSource") DataSource atollTrDataSource) {
        var adapter = new JdbcPollingChannelAdapter(atollTrDataSource, Queries.TRANSPORT_SITES);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }

    @Bean("rolloutSitesAdapter")
    public JdbcPollingChannelAdapter rolloutSitesAdapter(
        @Qualifier("ciptrackerDataSource") DataSource ciptrackerDataSource) {
        var adapter = new JdbcPollingChannelAdapter(ciptrackerDataSource, Queries.ROLLOUT_SITES);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }

    @Bean("cellsAdapter")
    public JdbcPollingChannelAdapter cellsAdapter(@Qualifier("postgresDataSource") DataSource postgresDataSource) {
        var adapter = new JdbcPollingChannelAdapter(postgresDataSource, Queries.CELLS);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }

    @Bean("azimuthHeightAdapter")
    public JdbcPollingChannelAdapter azimuthHeightAdapter(@Qualifier("atollDataSource") DataSource atollDataSource) {
        var adapter = new JdbcPollingChannelAdapter(atollDataSource, Queries.AZIMUTH_AND_HEIGHT);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }

    @Bean("cellInterferenceAdapter")
    public JdbcPollingChannelAdapter cellInterferenceAdapter(@Qualifier("mariaDataSource") DataSource mariaDataSource) {
        var adapter = new JdbcPollingChannelAdapter(mariaDataSource, Queries.CELL_INTERFERENCE);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }

    @Bean("siteWorksAdapter")
    public JdbcPollingChannelAdapter siteWorksAdapter(
        @Qualifier("ciptrackerDataSource") DataSource ciptrackerDataSource) {
        var adapter = new JdbcPollingChannelAdapter(ciptrackerDataSource, Queries.SITE_WORKS);
        adapter.setRowMapper(new ColumnMapRowMapper());
        return adapter;
    }
}
