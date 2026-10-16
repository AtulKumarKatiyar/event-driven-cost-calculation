package com.example.cost.aggregator;

import com.example.cost.contracts.Component;
import com.example.cost.contracts.Events.*;
import java.util.*;

public class AggregationState {
  public Requested requested;
  public Map<Component, Result> results = new EnumMap<>(Component.class);
  public boolean terminal;
  public long firstSeen;
}
