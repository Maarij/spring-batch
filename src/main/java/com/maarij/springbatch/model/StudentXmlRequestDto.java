package com.maarij.springbatch.model;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@XmlRootElement(name = "student")
public class StudentXmlRequestDto {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
}
