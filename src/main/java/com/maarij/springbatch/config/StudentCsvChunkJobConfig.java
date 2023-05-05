package com.maarij.springbatch.config;

import com.maarij.springbatch.listener.SkipListener;
import com.maarij.springbatch.model.StudentCsvRequestDto;
import com.maarij.springbatch.processor.StudentCsvItemProcessor;
import com.maarij.springbatch.writer.StudentCsvRequestDtoWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Date;

@Configuration
public class StudentCsvChunkJobConfig {

    private final StudentCsvRequestDtoWriter studentCsvRequestDtoWriter;
    private final StudentCsvItemProcessor studentCsvItemProcessor;
    private final SkipListener skipListener;

    public StudentCsvChunkJobConfig(StudentCsvRequestDtoWriter studentCsvRequestDtoWriter,
                                    StudentCsvItemProcessor studentCsvItemProcessor,
                                    SkipListener skipListener) {
        this.studentCsvRequestDtoWriter = studentCsvRequestDtoWriter;
        this.studentCsvItemProcessor = studentCsvItemProcessor;
        this.skipListener = skipListener;
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
                .processor(studentCsvItemProcessor)
//                .writer(studentCsvRequestDtoWriter)
                .writer(flatFileItemWriter(null))
                .faultTolerant()
                .skip(Throwable.class)
//                .skip(FlatFileParseException.class)
//                .skipLimit(Integer.MAX_VALUE)
                .skipPolicy(new AlwaysSkipItemSkipPolicy())
                .listener(skipListener)
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

    @Bean
    @StepScope
    public FlatFileItemWriter<StudentCsvRequestDto> flatFileItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {

        FlatFileItemWriter<StudentCsvRequestDto> flatFileItemWriter = new FlatFileItemWriter<>();
        flatFileItemWriter.setResource(fileSystemResource);

        flatFileItemWriter.setHeaderCallback(writer -> writer.write("Id,First Name,Last Name,Email"));

        BeanWrapperFieldExtractor<StudentCsvRequestDto> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"id", "firstName", "lastName", "email"});

        DelimitedLineAggregator<StudentCsvRequestDto> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setFieldExtractor(fieldExtractor);
        flatFileItemWriter.setLineAggregator(lineAggregator);

        flatFileItemWriter.setFooterCallback(writer -> writer.write("Created at " + new Date()));

        return flatFileItemWriter;
    }
}
