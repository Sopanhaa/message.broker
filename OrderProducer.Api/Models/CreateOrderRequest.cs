using Shared.Contracts.Events;

namespace OrderProducer.Api.Models;

public sealed record CreateOrderRequest(string? CustomerName, decimal TotalAmount,
    IReadOnlyList<OrderProduct>? Products = null)
{
    public Dictionary<string, string[]> Validate()
    {
        var errors = new Dictionary<string, string[]>();
        if (string.IsNullOrWhiteSpace(CustomerName) || CustomerName.Trim().Length > 100)
            errors[nameof(CustomerName)] = ["Customer name must contain 1 to 100 non-whitespace characters (after trimming)."];
        if (TotalAmount <= 0)
            errors[nameof(TotalAmount)] = ["Total amount must be greater than zero."];
        if (!OrderProduct.IsValid(Products, TotalAmount))
            errors[nameof(Products)] = ["Products must contain at least one item, each with a name of 1 to 100 characters after trimming, a positive quantity and a positive unit price. Their combined total must equal totalAmount."];
        return errors;
    }
}
