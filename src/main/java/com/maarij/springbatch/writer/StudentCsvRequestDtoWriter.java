package com.maarij.springbatch.writer;

import com.maarij.springbatch.model.StudentCsvRequestDto;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class StudentCsvRequestDtoWriter implements ItemWriter<StudentCsvRequestDto> {

    @Override
    public void write(Chunk<? extends StudentCsvRequestDto> chunk) {
        System.out.println("Inside Item Writer");
        chunk.getItems().forEach(System.out::println);
    }
}
