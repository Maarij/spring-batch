package com.maarij.springbatch.processor;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class FirstItemProcessor implements ItemProcessor<Integer, Long> {

    @Override
    public Long process(Integer item) {
        System.out.println("Inside Item Processor");
        return (long) (item + 10);
    }

}
