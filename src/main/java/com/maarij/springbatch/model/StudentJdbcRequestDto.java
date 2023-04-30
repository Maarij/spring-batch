package com.maarij.springbatch.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class StudentJdbcRequestDto {
    private Long id;
    private String firstName;
    private String lastName;
    private String email;
}
