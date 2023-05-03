package com.maarij.springbatch.config;

import com.maarij.springbatch.model.StudentXmlRequestDto;
import com.maarij.springbatch.writer.StudentXmlRequestDtoWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.StaxEventItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class StudentXmlChunkJobConfig {

    private final StudentXmlRequestDtoWriter studentXmlRequestDtoWriter;

    public StudentXmlChunkJobConfig(StudentXmlRequestDtoWriter studentXmlRequestDtoWriter) {
        this.studentXmlRequestDtoWriter = studentXmlRequestDtoWriter;
    }

    @Bean
    public Job studentXmlChunkJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("studentXmlChunkJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep(jobRepository, transactionManager))
                .build();
    }

    private Step firstChunkStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("First Chunk Step", jobRepository)
                .<StudentXmlRequestDto, StudentXmlRequestDto>chunk(3, transactionManager)
                .reader(staxEventItemReader(null))
//                .writer(studentXmlRequestDtoWriter)
                .writer(staxEventItemWriter(null))
                .build();
    }

    @Bean
    @StepScope
    public StaxEventItemReader<StudentXmlRequestDto> staxEventItemReader(
            @Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource) {

        StaxEventItemReader<StudentXmlRequestDto> staxEventItemReader = new StaxEventItemReader<>();

        staxEventItemReader.setResource(fileSystemResource);
        staxEventItemReader.setFragmentRootElementName("student");

        Jaxb2Marshaller unmarshaller = new Jaxb2Marshaller();
        unmarshaller.setClassesToBeBound(StudentXmlRequestDto.class);
        staxEventItemReader.setUnmarshaller(unmarshaller);


        return staxEventItemReader;
    }

    public StaxEventItemWriter<StudentXmlRequestDto> staxEventItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource) {
        StaxEventItemWriter<StudentXmlRequestDto> staxEventItemWriter = new StaxEventItemWriter<>();

        staxEventItemWriter.setResource(fileSystemResource);
        staxEventItemWriter.setRootTagName("students");

        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(StudentXmlRequestDto.class);
        staxEventItemWriter.setMarshaller(marshaller);


        return staxEventItemWriter;
    }
}
