package com.example.springbatch;

import java.util.List;

public class DailySensorData {

    private String data;

    private List<Double> measurements;

    public DailySensorData(String data, List<Double> measurements) {
        this.data = data;
        this.measurements = measurements;
    }

    public String getData() {
        return data;
    }

    public List<Double> getMeasurements() {
        return measurements;
    }
}
