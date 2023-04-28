package com.maarij.springbatch.config;

import com.maarij.springbatch.listener.FirstJobListener;
import com.maarij.springbatch.listener.FirstStepListener;
import com.maarij.springbatch.service.SecondTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class SampleJob {

    private final SecondTasklet firstTasklet;
    private final SecondTasklet secondTasklet;
    private final FirstJobListener firstJobListener;
    private final FirstStepListener firstStepListener;

    public SampleJob(SecondTasklet firstTasklet,
                     SecondTasklet secondTasklet,
                     FirstJobListener firstJobListener,
                     FirstStepListener firstStepListener) {
        this.firstTasklet = firstTasklet;
        this.secondTasklet = secondTasklet;
        this.firstJobListener = firstJobListener;
        this.firstStepListener = firstStepListener;
    }

    @Bean
    public Job firstJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("firstJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(firstStep(jobRepository, transactionManager))
                .next(secondStep(jobRepository, transactionManager))
                .listener(firstJobListener)
                .build();
    }

    private Step firstStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("firstStep", jobRepository)
                .tasklet(firstTasklet, transactionManager)
                .listener(firstStepListener)
                .build();
    }

    private Step secondStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("secondStep", jobRepository)
                .tasklet(secondTasklet, transactionManager)
                .build();
    }
}
