package kz.tele2.bts.radio.db;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Configuration
public class DatasourceConfiguration {

    @Primary
    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.oracle")
    public DataSource oracleDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Primary
    @Bean
    public JdbcTemplate oracleJdbcTemplate(){
        return new JdbcTemplate(oracleDataSource());
    }
    @Primary
    @Bean
    public NamedParameterJdbcTemplate oracleNamedParameterJdbcTemplate(){
        return new NamedParameterJdbcTemplate(oracleDataSource());
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.atoll")
    public DataSource atollDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public JdbcTemplate atollJdbcTemplate(){
        return new JdbcTemplate(atollDataSource());
    }
    @Bean
    public NamedParameterJdbcTemplate atollNamedParameterJdbcTemplate(){
        return new NamedParameterJdbcTemplate(atollDataSource());
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.atoll-tr")
    public DataSource atollTrDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public JdbcTemplate atollTrJdbcTemplate(){
        return new JdbcTemplate(atollTrDataSource());
    }
    @Bean
    public NamedParameterJdbcTemplate atollTrNamedParameterJdbcTemplate(){
        return new NamedParameterJdbcTemplate(atollTrDataSource());
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.maria")
    public DataSource mariaDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public JdbcTemplate mariaJdbcTemplate(){
        return new JdbcTemplate(mariaDataSource());
    }
    @Bean
    public NamedParameterJdbcTemplate mariaNamedParameterJdbcTemplate(){
        return new NamedParameterJdbcTemplate(mariaDataSource());
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.postgres")
    public DataSource postgresDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public JdbcTemplate postgresJdbcTemplate(){
        return new JdbcTemplate(postgresDataSource());
    }

    @Bean
    public NamedParameterJdbcTemplate postgresNamedParameterJdbcTemplate(){
        return new NamedParameterJdbcTemplate(postgresDataSource());
    }

    @Bean
    @ConfigurationProperties(prefix = "spring.datasource.ciptracker")
    public DataSource ciptrackerDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public JdbcTemplate ciptrackerJdbcTemplate(){
        return new JdbcTemplate(ciptrackerDataSource());
    }
    @Bean
    public NamedParameterJdbcTemplate ciptrackerNamedParameterJdbcTemplate(){
        return new NamedParameterJdbcTemplate(ciptrackerDataSource());
    }
}