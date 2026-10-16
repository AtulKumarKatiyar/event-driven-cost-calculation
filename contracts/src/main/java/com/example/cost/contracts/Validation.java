package com.example.cost.contracts;

import com.example.cost.contracts.Events.*;
import java.math.BigDecimal;
import java.util.EnumSet;

public final class Validation {
  private Validation() {}

  public static void finalEvent(Event<Final> event) {
    Final value = event.payload();
    if (value.state() == null
        || !value.state().terminal()
        || value.currency() == null
        || value.finishedAt() == null
        || value.components() == null) throw new IllegalArgumentException("Invalid terminal event");
    String expected =
        value.state() == State.COMPLETED ? "CalculationCompletedEvent" : "CalculationFailedEvent";
    if (!expected.equals(event.eventType()))
      throw new IllegalArgumentException("Event type and state disagree");
    if (value.state() == State.FAILED) {
      if (value.totalCost() != null || value.error() == null || !value.components().isEmpty())
        throw new IllegalArgumentException("Failed calculation cannot have a total");
    } else {
      if (value.error() != null
          || value.totalCost() == null
          || !value.components().keySet().equals(EnumSet.allOf(Component.class)))
        throw new IllegalArgumentException("Completed calculation requires all components");
      BigDecimal sum = BigDecimal.ZERO;
      for (BigDecimal amount : value.components().values()) {
        if (amount == null || amount.signum() < 0 || amount.scale() > 2)
          throw new IllegalArgumentException("Invalid amount");
        sum = sum.add(amount);
      }
      if (sum.compareTo(value.totalCost()) != 0)
        throw new IllegalArgumentException("Incorrect total");
    }
  }
}
