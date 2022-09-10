package com.example.demo.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import com.example.demo.model.Student;

@Configuration
@EnableBatchProcessing
public class StudentBatchConfiguration {
	
	@Autowired
	private StepBuilderFactory stepBuildeFactory;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private DataSource dataSource;
	
    @Bean
    public FlatFileItemReader<Student> readFromCsv(){
    	FlatFileItemReader<Student> reader = new FlatFileItemReader<Student>();
    	reader.setResource(new FileSystemResource("D://workspace/SpringBatchCsvToDB/file/csv_input.csv"));
    	reader.setLineMapper(new DefaultLineMapper<Student>() {
    		{
    			setLineTokenizer(new DelimitedLineTokenizer() {
    				{
    					setNames(Student.fields());
    				}
    			});
    			setFieldSetMapper(new  BeanWrapperFieldSetMapper<>() {
    				{
    				   setTargetType(Student.class);	
    				}
    			});
    			
    		}
    	});
    	return reader;
    	//reader.setResource(new ClassPathResource(""));
		//return null;
    	
    }
    @Bean
    public JdbcBatchItemWriter<Student> writerIntoDB(){
    	JdbcBatchItemWriter<Student> writer = new JdbcBatchItemWriter<Student>();
    	writer.setDataSource(dataSource);
    	writer.setSql("insert into csvtodata(id,firstName,lastName,email) values(:id,:firstName,:lastName,:email)");
    	writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Student>());
		return writer;
    }
    
    @Bean
    public Step step() {
    	
    	return stepBuildeFactory.get("step").<Student, Student>chunk(10)
    	.reader(readFromCsv()).writer(writerIntoDB()).build();
    }
    
    @Bean
    public Job job() {
    	
    	return jobBuilderFactory.get("job").flow(step()).end().build();
    }
}
