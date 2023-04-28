package com.maarij.springbatch.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class FirstJobListener implements JobExecutionListener {

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.printf("Before Job %s%n", jobExecution.getJobInstance().getJobName());
        System.out.printf("Job Params %s%n", jobExecution.getJobParameters());
        System.out.printf("Job Exec Context %s%n", jobExecution.getExecutionContext());

        jobExecution.getExecutionContext().put("key", "value");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.printf("After Job %s%n", jobExecution.getJobInstance().getJobName());
        System.out.printf("Job Params %s%n", jobExecution.getJobParameters());
        System.out.printf("Job Exec Context %s%n", jobExecution.getExecutionContext());
    }
}
