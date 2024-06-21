package com.example.ms_java_spring_batch_demo.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BatchController {
    private final JobLauncher jobLauncher;
    private final Job importUserJob;
    private final Job etlJob;

    public BatchController(JobLauncher jobLauncher, Job importUserJob, Job etlJob) {
        this.jobLauncher = jobLauncher;
        this.importUserJob = importUserJob;
        this.etlJob = etlJob;
    }

    @GetMapping("/startImportUserJob")
    public String startImportUserJob() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addString("JobID", String.valueOf(System.currentTimeMillis()))
                    .toJobParameters();
            jobLauncher.run(importUserJob, params);
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
        return "Import User Job Started";
    }

    @GetMapping("/startEtlJob")
    public String startEtlJob() {
        try {
            JobParameters params = new JobParametersBuilder()
                    .addString("JobID", String.valueOf(System.currentTimeMillis()))
                    .toJobParameters();
            jobLauncher.run(etlJob, params);
        } catch (Exception e) {
            return "Error: " + e.getMessage();
        }
        return "ETL Job Started";
    }
}
