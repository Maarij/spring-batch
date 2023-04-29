package com.maarij.springbatch.service;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
public class JobService {

    private final JobLauncher jobLauncher;

    private final Job taskletJob;
    private final Job chunkJob;
    private final Job studentCsvChunkJob;
    private final Job studentJsonChunkJob;

    public JobService(JobLauncher jobLauncher,
                      Job taskletJob,
                      Job chunkJob,
                      Job studentCsvChunkJob,
                      Job studentJsonChunkJob) {
        this.jobLauncher = jobLauncher;
        this.taskletJob = taskletJob;
        this.chunkJob = chunkJob;
        this.studentCsvChunkJob = studentCsvChunkJob;
        this.studentJsonChunkJob = studentJsonChunkJob;
    }

    @Async
    public void startJob(String jobName) {
        Map<String, JobParameter<?>> params = new HashMap<>();
        params.put("currentTime", new JobParameter<>(System.currentTimeMillis(), Long.class));

        JobParameters jobParameters = new JobParameters(params);

        try {
            JobExecution jobExecution;

            switch (jobName) {
                case "taskletJob"           -> jobExecution = jobLauncher.run(taskletJob, jobParameters);
                case "chunkJob"             -> jobExecution = jobLauncher.run(chunkJob, jobParameters);
                case "studentCsvChunkJob"   -> jobExecution = jobLauncher.run(studentCsvChunkJob, jobParameters);
                case "studentJsonChunkJob"  -> jobExecution = jobLauncher.run(studentJsonChunkJob, jobParameters);
                default                     -> throw new IllegalArgumentException("Invalid job name");
            }

            System.out.println("jobExecution = " + jobExecution.getId());
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("Exception starting job %s", jobName));
        }
    }
}
