package com.maarij.springbatch.processor;

import com.maarij.springbatch.model.StudentCsvRequestDto;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class StudentCsvItemProcessor implements ItemProcessor<StudentCsvRequestDto, StudentCsvRequestDto> {
    @Override
    public StudentCsvRequestDto process(StudentCsvRequestDto item) {
        System.out.println(item.getId());

        return item;
    }
}
