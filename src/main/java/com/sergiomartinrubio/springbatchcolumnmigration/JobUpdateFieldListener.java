package com.sergiomartinrubio.springbatchcolumnmigration;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class JobUpdateFieldListener extends JobExecutionListenerSupport {

    private static final String SQL_SELECT_SECOND_TABLE = "SELECT * FROM database.second_table";
    private final JdbcTemplate jdbcTemplate;

    @Override
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            jdbcTemplate.query(SQL_SELECT_SECOND_TABLE,
                (rs, row) -> new SecondTable(
                    rs.getString(2),
                    rs.getString(3))
            ).forEach(updateField -> log.info("Updated fieldadded -> " + updateField));
            log.info("COPY IS DONE!!!");
        }
    }
}
