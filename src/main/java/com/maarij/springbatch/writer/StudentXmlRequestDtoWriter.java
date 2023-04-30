package com.maarij.springbatch.writer;

import com.maarij.springbatch.model.StudentXmlRequestDto;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

@Component
public class StudentXmlRequestDtoWriter implements ItemWriter<StudentXmlRequestDto> {

    @Override
    public void write(Chunk<? extends StudentXmlRequestDto> chunk) {
        System.out.println("Inside Item Writer");
        chunk.getItems().forEach(System.out::println);
    }
}
