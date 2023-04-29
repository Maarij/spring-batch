package com.maarij.springbatch.config;

import com.maarij.springbatch.model.StudentJsonRequestDto;
import com.maarij.springbatch.writer.StudentJsonRequestDtoWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class StudentJsonChunkJobConfig {

    private final StudentJsonRequestDtoWriter studentJsonRequestDtoWriter;

    public StudentJsonChunkJobConfig(StudentJsonRequestDtoWriter studentJsonRequestDtoWriter) {
        this.studentJsonRequestDtoWriter = studentJsonRequestDtoWriter;
    }

    @Bean
    public Job studentJsonChunkJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("studentJsonChunkJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep(jobRepository, transactionManager))
                .build();
    }

    private Step firstChunkStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("First Chunk Step", jobRepository)
                .<StudentJsonRequestDto, StudentJsonRequestDto>chunk(3, transactionManager)
                .reader(jsonItemReader(null))
                .writer(studentJsonRequestDtoWriter)
                .build();
    }

    @Bean
    @StepScope
    public JsonItemReader<StudentJsonRequestDto> jsonItemReader(
            @Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {

        JsonItemReader<StudentJsonRequestDto> jsonItemReader = new JsonItemReader<>();

        jsonItemReader.setResource(fileSystemResource);
        jsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJsonRequestDto.class));

        return jsonItemReader;
    }
}
