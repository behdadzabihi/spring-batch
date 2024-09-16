package com.example.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobExecution;

@Component
public class JobRunner implements CommandLineRunner {
    Logger logger = LoggerFactory.getLogger(JobRunner.class);
    private final JobLauncher jobLauncher;
    private final Job temperatureSensorJob;

    public JobRunner(JobLauncher jobLauncher, Job temperatureSensorJob) {
        this.jobLauncher = jobLauncher;
        this.temperatureSensorJob = temperatureSensorJob;
    }

    @Override
    public void run(String... args) throws Exception {
        JobExecution jobExecution = jobLauncher.run(temperatureSensorJob, new JobParameters());
        logger.info("Job Status: " + jobExecution.getStatus());
        System.out.println("Job Status: " + jobExecution.getStatus());
    }
}
