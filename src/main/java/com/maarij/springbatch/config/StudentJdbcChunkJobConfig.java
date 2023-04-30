package com.maarij.springbatch.config;

import com.maarij.springbatch.model.StudentJdbcRequestDto;
import com.maarij.springbatch.writer.StudentJdbcRequestDtoWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
public class StudentJdbcChunkJobConfig {

    private final DataSource dataSource;
    private final StudentJdbcRequestDtoWriter studentJdbcRequestDtoWriter;

    // Will need to set spring.datasource.jdbc-url
//    @Bean
//    @ConfigurationProperties(prefix = "spring.otherdatasource")
//    public DataSource otherdataSource() {
//        return DataSourceBuilder.create().build();
//    }

    public StudentJdbcChunkJobConfig(DataSource dataSource, StudentJdbcRequestDtoWriter studentJdbcRequestDtoWriter) {
        this.dataSource = dataSource;
        this.studentJdbcRequestDtoWriter = studentJdbcRequestDtoWriter;
    }

    @Bean
    public Job studentJdbcChunkJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("studentJdbcChunkJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep(jobRepository, transactionManager))
                .build();
    }

    private Step firstChunkStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("First Chunk Step", jobRepository)
                .<StudentJdbcRequestDto, StudentJdbcRequestDto>chunk(3, transactionManager)
                .reader(jdbcCursorItemReader())
                .writer(studentJdbcRequestDtoWriter)
                .build();
    }

    @Bean
    @StepScope
    public JdbcCursorItemReader<StudentJdbcRequestDto> jdbcCursorItemReader() {

        JdbcCursorItemReader<StudentJdbcRequestDto> jdbcCursorItemReader = new JdbcCursorItemReader<>();

        jdbcCursorItemReader.setDataSource(dataSource);

        String selectQuery = "select id, first_name as firstName, last_name as lastName, email from student";
        jdbcCursorItemReader.setSql(selectQuery);

        BeanPropertyRowMapper<StudentJdbcRequestDto> rowMapper = new BeanPropertyRowMapper<>();
        rowMapper.setMappedClass(StudentJdbcRequestDto.class);
        jdbcCursorItemReader.setRowMapper(rowMapper);

        return jdbcCursorItemReader;
    }
}
