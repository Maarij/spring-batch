package com.maarij.springbatch.service;

import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class JobService {

    private final JobLauncher jobLauncher;

    @Qualifier("firstJob")
    private final Job firstJob;

    @Qualifier("secondJob")
    private final Job secondJob;

    public JobService(JobLauncher jobLauncher, Job firstJob, Job secondJob) {
        this.jobLauncher = jobLauncher;
        this.firstJob = firstJob;
        this.secondJob = secondJob;
    }

    @Async
    public void startJob(String jobName) {
        Map<String, JobParameter<?>> params = new HashMap<>();
        params.put("currentTime", new JobParameter<>(System.currentTimeMillis(), Long.class));

        JobParameters jobParameters = new JobParameters(params);

        try {
            JobExecution jobExecution = null;

            if (StringUtils.equals("firstJob", jobName)) {
                jobExecution = jobLauncher.run(firstJob, jobParameters);
            } else if (StringUtils.equals("secondJob", jobName)) {
                jobExecution = jobLauncher.run(secondJob, jobParameters);
            }

            System.out.println("jobExecution = " + jobExecution.getId());
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("Exception starting job %s", jobName));
        }
    }
}
