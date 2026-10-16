package com.example.cost.contracts;

public enum Component {
  FIRST_COST("first"),
  FREIGHT_COST("freight"),
  HANDLING_COST("handling"),
  INVENTORY_COST("inventory");
  private final String slug;

  Component(String slug) {
    this.slug = slug;
  }

  public String requestTopic() {
    return "cost." + slug + ".requested.v1";
  }

  public String resultTopic() {
    return "cost." + slug + ".result.v1";
  }
}
