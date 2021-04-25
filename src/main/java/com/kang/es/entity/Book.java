package com.kang.es.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * @author kang
 * @version 1.0
 * @date 2020/4/22 11:36
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Book {

    private Integer id;

    private Long number;

    private LocalDateTime create_time;

    private Double price;

    private String name;

    private String title;
}
