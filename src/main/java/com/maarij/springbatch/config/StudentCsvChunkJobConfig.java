package com.maarij.springbatch.config;

import com.maarij.springbatch.model.StudentCsvRequestDto;
import com.maarij.springbatch.writer.StudentCsvRequestDtoWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class StudentCsvChunkJobConfig {

    private final StudentCsvRequestDtoWriter studentCsvRequestDtoWriter;

    public StudentCsvChunkJobConfig(StudentCsvRequestDtoWriter studentCsvRequestDtoWriter) {
        this.studentCsvRequestDtoWriter = studentCsvRequestDtoWriter;
    }

    @Bean
    public Job studentCsvChunkJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("studentCsvChunkJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep(jobRepository, transactionManager))
                .build();
    }

    private Step firstChunkStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("First Chunk Step", jobRepository)
                .<StudentCsvRequestDto, StudentCsvRequestDto>chunk(3, transactionManager)
                .reader(flatFileItemReader(null))
                .writer(studentCsvRequestDtoWriter)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemReader<StudentCsvRequestDto> flatFileItemReader(
            @Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {

        FlatFileItemReader<StudentCsvRequestDto> flatFileItemReader = new FlatFileItemReader<>();

        flatFileItemReader.setResource(fileSystemResource);
        flatFileItemReader.setLineMapper(createStudentLineMapper());
        flatFileItemReader.setLinesToSkip(1);

        return flatFileItemReader;
    }

    private DefaultLineMapper<StudentCsvRequestDto> createStudentLineMapper() {
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("ID", "First Name", "Last Name", "Email");

        BeanWrapperFieldSetMapper<StudentCsvRequestDto> fieldMapper = new BeanWrapperFieldSetMapper<>();
        fieldMapper.setTargetType(StudentCsvRequestDto.class);

        DefaultLineMapper<StudentCsvRequestDto> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldMapper);
        return lineMapper;
    }
}