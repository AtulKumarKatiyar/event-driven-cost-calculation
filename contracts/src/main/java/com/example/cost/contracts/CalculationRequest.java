package com.example.cost.contracts;

import jakarta.validation.constraints.*;
import java.util.UUID;

public record CalculationRequest(
    @NotNull UUID calculationId,
    @NotBlank @Size(max = 100) String rp,
    @NotBlank @Size(max = 100) String mp,
    @NotBlank @Size(max = 100) String vbu,
    @NotBlank @Size(max = 100) String financeGroup,
    @NotBlank @Size(max = 100) String productId,
    @Min(1) @Max(1000000) int quantity) {
  public CalculationRequest normalized() {
    return new CalculationRequest(
        calculationId,
        rp.strip(),
        mp.strip(),
        vbu.strip(),
        financeGroup.strip(),
        productId.strip(),
        quantity);
  }
}
