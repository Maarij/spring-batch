package com.maarij.springbatch.writer;

import com.maarij.springbatch.model.StudentJsonRequestDto;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class StudentJsonRequestDtoWriter implements ItemWriter<StudentJsonRequestDto> {

    @Override
    public void write(Chunk<? extends StudentJsonRequestDto> chunk) {
        System.out.println("Inside Item Writer");
        chunk.getItems().forEach(System.out::println);
    }
}
