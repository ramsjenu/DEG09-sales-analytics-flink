package SalesAnalysis.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {
    public Integer productId;
    public String name;
    public String description;
    public String category;
    public Float price;
}
