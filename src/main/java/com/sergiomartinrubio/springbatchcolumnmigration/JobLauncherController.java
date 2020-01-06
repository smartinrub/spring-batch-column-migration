package com.sergiomartinrubio.springbatchcolumnmigration;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/batch")
public class JobLauncherController {

    private final Job copyUpdateFieldJob;
    private final JobLauncher jobLauncher;

    @Autowired
    public JobLauncherController(final Job copyUpdateFieldJob, final JobLauncher jobLauncher) {
        this.copyUpdateFieldJob = copyUpdateFieldJob;
        this.jobLauncher = jobLauncher;
    }

    @RequestMapping("/start")
    public String startMigrationJob() {
        JobExecution execution;
        try {
            execution = jobLauncher.run(copyUpdateFieldJob, new JobParameters());
            log.info("Job Status : {}", execution.getStatus());
            log.info("Job completed");
            return execution.getStatus().toString();
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
            JobParametersInvalidException e) {
            e.printStackTrace();
            log.error("Job failed");
            return "Job failed";
        }
    }

}
