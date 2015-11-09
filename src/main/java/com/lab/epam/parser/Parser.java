package com.lab.epam.parser;

import com.lab.epam.hadoop.task3.counterEnum.CounterName;

/**
 * Created by Bohdan-Dmytro_Vovchu on 11/9/2015.
 */
public class Parser {

    public static CounterName containString(String text){

        CounterName[] values = CounterName.values();
        for (CounterName value : values) {
            if(text.toUpperCase().contains(value.toString())){
                return value;
            }
        }

        return CounterName.OTHERS;
    }
}
