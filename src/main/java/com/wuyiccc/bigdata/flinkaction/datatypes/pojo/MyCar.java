package com.wuyiccc.bigdata.flinkaction.datatypes.pojo;

/**
 * @author wuyiccc
 * @date 2022/3/2 23:55
 */
public class MyCar {

    private String brand;
    private int amount;

    public MyCar() {
    }

    public MyCar(String brand, int amount) {
        this.brand = brand;
        this.amount = amount;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "MyCar{" +
                "brand='" + brand + '\'' +
                ", amount=" + amount +
                '}';
    }
}
