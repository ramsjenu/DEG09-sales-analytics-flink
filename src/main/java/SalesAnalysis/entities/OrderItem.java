package SalesAnalysis.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderItem {
    public Integer orderItemId;
    public Integer orderId;
    public Integer productId;
    public Integer quantity;
    public Float pricePerUnit;
}
