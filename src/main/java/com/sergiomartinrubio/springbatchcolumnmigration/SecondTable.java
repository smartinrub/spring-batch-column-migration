package com.sergiomartinrubio.springbatchcolumnmigration;

import lombok.Value;

@Value
public class SecondTable {
    private final String firstTableId;
    private final String updateField;
}
