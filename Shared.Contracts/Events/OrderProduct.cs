namespace Shared.Contracts.Events;

public sealed record OrderProduct(string Name, int Quantity, decimal UnitPrice)
{
    public static bool IsValid(IReadOnlyList<OrderProduct>? products, decimal totalAmount)
    {
        if (products is null) return true;
        if (products.Count == 0) return false;

        decimal total = 0;
        try
        {
            foreach (var product in products)
            {
                if (product is null || string.IsNullOrWhiteSpace(product.Name) ||
                    product.Name.Trim().Length > 100 || product.Quantity <= 0 || product.UnitPrice <= 0)
                    return false;

                total += product.Quantity * product.UnitPrice;
            }
        }
        catch (OverflowException)
        {
            return false;
        }

        return total == totalAmount;
    }
}
