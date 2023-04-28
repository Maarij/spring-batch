package com.maarij.springbatch.listener;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
public class FirstStepListener implements StepExecutionListener {

    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.printf("Before Step %s%n", stepExecution.getStepName());
        System.out.printf("Job Execution Context %s%n", stepExecution.getJobExecution().getExecutionContext());
        System.out.printf("Step Execution Context %s%n", stepExecution.getExecutionContext());

        stepExecution.getExecutionContext().put("key", "value");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.printf("After Step %s%n", stepExecution.getStepName());
        System.out.printf("Job Execution Context %s%n", stepExecution.getJobExecution().getExecutionContext());
        System.out.printf("Step Execution Context %s%n", stepExecution.getExecutionContext());

        return ExitStatus.COMPLETED;
    }
}
