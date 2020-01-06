package com.sergiomartinrubio.springbatchcolumnmigration;

import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;

@Configuration
@AllArgsConstructor
public class BatchConfiguration {

    private static final String SQL_SELECT_FIRST_TABLE_QUERY = "SELECT id, update_field FROM database.first_table";
    private static final String SQL_UPDATE_SECOND_TABLE_QUERY = "UPDATE database.second_table SET update_field = :updateField WHERE first_table_id = :firstTableId";

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job copyUpdateFieldJob(JobUpdateFieldListener listener, Step copyUpdateFieldStep) {
        return jobBuilderFactory.get("copyUpdateFieldJob")
            .listener(listener)
            .start(copyUpdateFieldStep)
            .build();
    }

    @Bean
    public Step copyUpdateFieldStep(JdbcCursorItemReader<FirstTable> reader,
                                         JdbcBatchItemWriter<SecondTable> writer) {
        return stepBuilderFactory.get("copyUpdateFieldStep")
            .<FirstTable, SecondTable>chunk(4)
            .reader(reader)
            .writer(writer)
            .faultTolerant()
            .skipPolicy(new ItemVerificationSkipper())
            .build();
    }

    @Bean
    public JdbcCursorItemReader<FirstTable> firstTableReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<FirstTable>()
            .name("firstTableReader")
            .dataSource(dataSource)
            .sql(SQL_SELECT_FIRST_TABLE_QUERY)
            .rowMapper(new FirstTableRowMapper())
            .build();
    }

    @Bean
    public JdbcBatchItemWriter<SecondTable> aggregatesTableWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<SecondTable>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql(SQL_UPDATE_SECOND_TABLE_QUERY)
            .dataSource(dataSource)
            .build();
    }

    public static class FirstTableRowMapper implements RowMapper<FirstTable> {

        @Override
        public FirstTable mapRow(final ResultSet resultSet, final int i) throws SQLException {
            return new FirstTable(
                resultSet.getString("id"),
                resultSet.getString("update_field")
            );
        }
    }

}
