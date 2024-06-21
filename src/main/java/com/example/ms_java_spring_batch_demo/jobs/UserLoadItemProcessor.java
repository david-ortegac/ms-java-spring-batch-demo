package com.example.ms_java_spring_batch_demo.jobs;

import com.example.ms_java_spring_batch_demo.entity.User;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class UserLoadItemProcessor implements ItemProcessor<User, User> {
    @Override
    public User process(User user) throws Exception {
        if (!user.getEmail().contains("@")) {
            user.setEmail(user.getEmail() + "@default.com");
        }
        return user;
    }

    @Bean
    public JdbcBatchItemWriter<User> dbWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<User>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("SELECT * FROM users")
                .dataSource(dataSource)
                .build();
    }

    @Bean
    public Job etlJob(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
                      JdbcBatchItemWriter<User> writer, FlatFileItemReader<User> reader,
                      UserItemProcessor processor) {

        return (Job) new StepBuilder("step1", jobRepository)
                .<User, User>chunk(3, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }
}
