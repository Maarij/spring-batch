package com.maarij.springbatch.service.scheduler;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class JobScheduler {

    private final JobLauncher jobLauncher;

    @Qualifier("chunkJobConfig")
    private final Job chunkJob;

    public JobScheduler(JobLauncher jobLauncher, Job chunkJob) {
        this.jobLauncher = jobLauncher;
        this.chunkJob = chunkJob;
    }

//    @Scheduled(cron = "0 0/1 * 1/1 * ?")
    public void secondJobStarter() {
        Map<String, JobParameter<?>> params = new HashMap<>();
        params.put("currentTime", new JobParameter<>(System.currentTimeMillis(), Long.class));

        JobParameters jobParameters = new JobParameters(params);

        try {
            JobExecution jobExecution = jobLauncher.run(chunkJob, jobParameters);

            System.out.println("jobExecution = " + jobExecution);
        } catch (Exception ex) {
            throw new IllegalArgumentException("Exception starting second job");
        }
    }
}
