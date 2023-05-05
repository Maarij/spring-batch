package com.maarij.springbatch.listener;

import com.maarij.springbatch.model.StudentCsvRequestDto;
import org.springframework.batch.core.annotation.OnSkipInProcess;
import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;

@Component
public class SkipListener {

    @OnSkipInRead
    public void skipInRead(Throwable t) {
        if (t instanceof FlatFileParseException) {
            createFile("C:\\Git\\spring-batch\\exceptionFiles\\reader\\SkipInRead.txt",
                    ((FlatFileParseException) t).getInput());
        }
    }

    public void createFile(String filePath, String data) {
        try (FileWriter writer = new FileWriter(new File(filePath), true)) {
            writer.write(data + "," + new Date() + "\n");
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    @OnSkipInProcess
    public void skipInProcess(StudentCsvRequestDto i, Throwable t) {
        if (t instanceof NullPointerException) {
            createFile("C:\\Git\\spring-batch\\exceptionFiles\\reader\\SkipInProcess.txt",
                    i.toString());
        }
    }
}
