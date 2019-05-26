package com.catchu.sparksql;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

@Data
@Accessors(chain = true)
public class Person implements Serializable {
    private Long id;

    private String name;

    private Integer age;

}
