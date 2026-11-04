namespace OrderProducer.Api.Models;

public sealed record CreateOrderRequest(string? CustomerName, decimal TotalAmount)
{
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();
        if (string.IsNullOrWhiteSpace(CustomerName) || CustomerName.Trim().Length > 100)
            errors[nameof(CustomerName)] = ["Customer name must contain 1 to 100 non-whitespace characters (after trimming)."];
        if (TotalAmount <= 0)
            errors[nameof(TotalAmount)] = ["Total amount must be greater than zero."];
        return errors;
    }
}
