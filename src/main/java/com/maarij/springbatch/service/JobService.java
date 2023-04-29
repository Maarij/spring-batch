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

    @Qualifier("taskletJobConfig")
    private final Job taskletJob;

    @Qualifier("chunkJobConfig")
    private final Job chunkJob;

    public JobService(JobLauncher jobLauncher, Job taskletJob, Job chunkJob) {
        this.jobLauncher = jobLauncher;
        this.taskletJob = taskletJob;
        this.chunkJob = chunkJob;
    }

    @Async
    public void startJob(String jobName) {
        Map<String, JobParameter<?>> params = new HashMap<>();
        params.put("currentTime", new JobParameter<>(System.currentTimeMillis(), Long.class));

        JobParameters jobParameters = new JobParameters(params);

        try {
            JobExecution jobExecution = null;

            if (StringUtils.equals("taskletJob", jobName)) {
                jobExecution = jobLauncher.run(taskletJob, jobParameters);
            } else if (StringUtils.equals("chunkJob", jobName)) {
                jobExecution = jobLauncher.run(chunkJob, jobParameters);
            }

            System.out.println("jobExecution = " + jobExecution.getId());
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("Exception starting job %s", jobName));
        }
    }
}
