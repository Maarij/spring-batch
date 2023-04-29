package com.maarij.springbatch.config;

import com.maarij.springbatch.model.StudentCsvRequestDto;
import com.maarij.springbatch.processor.FirstItemProcessor;
import com.maarij.springbatch.reader.FirstItemReader;
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
public class StudentChunkJobConfig {

    private final FirstItemReader firstItemReader;
    private final FirstItemProcessor firstItemProcessor;
    private final StudentCsvRequestDtoWriter studentCsvRequestDtoWriter;

    public StudentChunkJobConfig(FirstItemReader firstItemReader,
                                 FirstItemProcessor firstItemProcessor,
                                 StudentCsvRequestDtoWriter studentCsvRequestDtoWriter) {
        this.firstItemReader = firstItemReader;
        this.firstItemProcessor = firstItemProcessor;
        this.studentCsvRequestDtoWriter = studentCsvRequestDtoWriter;
    }

    @Bean
    public Job studentChunkJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("studentChunkJob", jobRepository)
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

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("ID", "First Name", "Last Name", "Email");

        BeanWrapperFieldSetMapper<StudentCsvRequestDto> fieldMapper = new BeanWrapperFieldSetMapper<>();
        fieldMapper.setTargetType(StudentCsvRequestDto.class);

        DefaultLineMapper<StudentCsvRequestDto> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(fieldMapper);

        flatFileItemReader.setLineMapper(lineMapper);
        flatFileItemReader.setLinesToSkip(1);

        return flatFileItemReader;
    }
}
