package com.maarij.springbatch.config;

import com.maarij.springbatch.listener.FirstJobListener;
import com.maarij.springbatch.listener.FirstStepListener;
import com.maarij.springbatch.processor.FirstItemProcessor;
import com.maarij.springbatch.reader.FirstItemReader;
import com.maarij.springbatch.service.tasklet.SecondTasklet;
import com.maarij.springbatch.writer.FirstItemWriter;
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
    private final FirstItemReader firstItemReader;
    private final FirstItemProcessor firstItemProcessor;
    private final FirstItemWriter firstItemWriter;

    public SampleJob(SecondTasklet firstTasklet,
                     SecondTasklet secondTasklet,
                     FirstJobListener firstJobListener,
                     FirstStepListener firstStepListener,
                     FirstItemReader firstItemReader,
                     FirstItemProcessor firstItemProcessor,
                     FirstItemWriter firstItemWriter) {
        this.firstTasklet = firstTasklet;
        this.secondTasklet = secondTasklet;
        this.firstJobListener = firstJobListener;
        this.firstStepListener = firstStepListener;
        this.firstItemReader = firstItemReader;
        this.firstItemProcessor = firstItemProcessor;
        this.firstItemWriter = firstItemWriter;
    }

    @Bean
    public Job taskletJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("taskletJob", jobRepository)
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

    @Bean
    public Job chunkJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("chunkJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep(jobRepository, transactionManager))
                .build();
    }

    private Step firstChunkStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("First Chunk Step", jobRepository)
                .<Integer, Long>chunk(3, transactionManager)
                .reader(firstItemReader)
                .processor(firstItemProcessor)
                .writer(firstItemWriter)
                .build();
    }
}
