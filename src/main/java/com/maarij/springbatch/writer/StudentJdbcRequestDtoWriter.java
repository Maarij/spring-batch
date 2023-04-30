package com.maarij.springbatch.writer;

import com.maarij.springbatch.model.StudentJdbcRequestDto;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class StudentJdbcRequestDtoWriter implements ItemWriter<StudentJdbcRequestDto> {

    @Override
    public void write(Chunk<? extends StudentJdbcRequestDto> chunk) {
        System.out.println("Inside Item Writer");
        chunk.getItems().forEach(System.out::println);
    }
}
