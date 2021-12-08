package com.wuyiccc.bigdata.pojo;

/**
 * @author wuyiccc
 * @date 2021/12/8 23:27
 */
public class MyOrder {
    private Long id;
    private String product;
    private Integer amount;

    public MyOrder() {
    }

    public MyOrder(Long id, String product, Integer amount) {
        this.id = id;
        this.product = product;
        this.amount = amount;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public Integer getAmount() {
        return amount;
    }

    public void setAmount(Integer amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "MyOrder{" +
                "id=" + id +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                '}';
    }
}
