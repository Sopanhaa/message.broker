using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using OrderPersistence;
using OrderProducer.Api.Services;
using Shared.Contracts.Events;

namespace OrderMessagingSystem.Tests;

public sealed class OrdersApiTests
{
    [Theory]
    [InlineData("/")]
    [InlineData("/health")]
    public async Task Status_endpoints_work_without_a_broker(string path)
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.GetAsync(path);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Valid_order_publishes_event_and_returns_accepted()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.PostAsJsonAsync("/orders", new { customerName = " Sopanha ", totalAmount = 50m });
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var order = Assert.Single(app.Publisher.Orders);
        Assert.NotEqual(Guid.Empty, order.OrderId);
        Assert.Equal("Sopanha", order.CustomerName);
        Assert.Equal(50m, order.TotalAmount);
        Assert.InRange(order.CreatedAtUtc, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(order.OrderId, body.GetProperty("orderId").GetGuid());
    }

    [Fact]
    public async Task Order_products_are_trimmed_and_included_in_the_published_event()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.PostAsJsonAsync("/orders", new
        {
            customerName = "Sopanha",
            totalAmount = 50m,
            products = new[] { new OrderProduct(" Keyboard ", 2, 20m), new OrderProduct("Mouse", 1, 10m) }
        });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var order = Assert.Single(app.Publisher.Orders);
        Assert.Equal(new[] { new OrderProduct("Keyboard", 2, 20m), new OrderProduct("Mouse", 1, 10m) }, order.Products);
    }

    [Theory]
    [InlineData("[]")]
    [InlineData("[null]")]
    [InlineData("[{}]")]
    [InlineData("[{\"name\":\" \",\"quantity\":1,\"unitPrice\":50}]")]
    [InlineData("[{\"name\":\"Mouse\",\"quantity\":0,\"unitPrice\":50}]")]
    [InlineData("[{\"name\":\"Mouse\",\"quantity\":1,\"unitPrice\":-50}]")]
    [InlineData("[{\"name\":\"Mouse\",\"quantity\":1,\"unitPrice\":10}]")]
    [InlineData("[{\"name\":\"Mouse\",\"quantity\":2,\"unitPrice\":79228162514264337593543950335}]")]
    public async Task Invalid_products_return_bad_request_without_publishing(string products)
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        var json = "{\"customerName\":\"Sopanha\",\"totalAmount\":50,\"products\":" + products + "}";
        using var response = await client.PostAsync("/orders", new StringContent(json, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.Orders);
    }

    [Fact]
    public async Task Accepted_response_waits_until_publishing_is_confirmed()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var confirmed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        app.Publisher.OnPublish = async token =>
        {
            entered.SetResult();
            await confirmed.Task.WaitAsync(token);
        };
        var request = client.PostAsJsonAsync("/orders", new { customerName = "Sopanha", totalAmount = 50m });
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.False(request.IsCompleted);
        confirmed.SetResult();
        using var response = await request;
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
    }

    [Theory]
    [InlineData(null, 50)]
    [InlineData("", 50)]
    [InlineData("   ", 50)]
    [InlineData("Sopanha", 0)]
    [InlineData("Sopanha", -1)]
    public async Task Invalid_order_returns_bad_request_without_publishing(string? name, decimal total)
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.PostAsJsonAsync("/orders", new { customerName = name, totalAmount = total });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.Orders);
    }

    [Fact]
    public async Task Long_customer_name_returns_bad_request()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.PostAsJsonAsync("/orders", new { customerName = new string('x', 101), totalAmount = 50m });
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.Orders);
    }

    [Theory]
    [InlineData("{broken")]
    [InlineData("null")]
    [InlineData("{}")]
    [InlineData("{\"customerName\":\"Sopanha\",\"totalAmount\":\"not-a-number\"}")]
    public async Task Invalid_json_or_missing_fields_returns_bad_request(string json)
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.PostAsync("/orders", new StringContent(json, Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.Orders);
    }

    [Fact]
    public async Task Publish_failure_returns_service_unavailable()
    {
        await using var app = new ApiFactory();
        app.Publisher.OnPublish = _ => throw new MessagePublishException("Not confirmed", new IOException("broker down"));
        using var client = app.CreateClient();
        using var response = await client.PostAsJsonAsync("/orders", new { customerName = "Sopanha", totalAmount = 50m });
        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [Fact]
    public async Task Valid_status_update_publishes_event_and_returns_accepted()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        var orderId = await CreateOrderAsync(client);
        using var response = await client.PatchAsJsonAsync($"/orders/{orderId}/status",
            new { status = "Completed", detail = " Delivered " });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var status = Assert.Single(app.Publisher.Statuses);
        Assert.Equal(orderId, status.OrderId);
        Assert.Equal(OrderStatus.Completed, status.Status);
        Assert.Equal("Delivered", status.Detail);
        Assert.InRange(status.UpdatedAtUtc, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("Completed", body.GetProperty("status").GetString());
    }

    [Theory]
    [InlineData("{}")]
    [InlineData("{\"status\":999}")]
    [InlineData("{\"status\":\"Unknown\"}")]
    public async Task Invalid_status_update_returns_bad_request_without_publishing(string json)
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.PatchAsync($"/orders/{Guid.NewGuid()}/status",
            new StringContent(json, Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.Statuses);
    }

    [Fact]
    public async Task Empty_order_id_returns_bad_request_without_publishing()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.PatchAsJsonAsync($"/orders/{Guid.Empty}/status",
            new { status = "Completed" });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.Statuses);
    }

    [Fact]
    public async Task Long_status_detail_returns_bad_request_without_publishing()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var response = await client.PatchAsJsonAsync($"/orders/{Guid.NewGuid()}/status",
            new { status = "Failed", detail = new string('x', OrderStatusChanged.MaxDetailLength + 1) });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.Statuses);
    }

    [Fact]
    public async Task Status_publish_failure_returns_service_unavailable()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        var orderId = await CreateOrderAsync(client);
        app.Publisher.OnPublishStatus = _ => throw new MessagePublishException("Not confirmed", new IOException("broker down"));
        using var response = await client.PatchAsJsonAsync($"/orders/{orderId}/status",
            new { status = "Failed" });

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [Fact]
    public async Task Get_order_returns_persisted_state()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        var orderId = await CreateOrderAsync(client);

        using var response = await client.GetAsync($"/orders/{orderId}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(orderId, body.GetProperty("orderId").GetGuid());
        Assert.Equal("Sopanha", body.GetProperty("customerName").GetString());
        Assert.Equal(50m, body.GetProperty("totalAmount").GetDecimal());
        Assert.Equal("Received", body.GetProperty("status").GetString());
        Assert.Empty(body.GetProperty("products").EnumerateArray());
    }

    [Fact]
    public async Task Get_order_returns_persisted_products()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();
        using var created = await client.PostAsJsonAsync("/orders", new
        {
            customerName = "Sopanha",
            totalAmount = 50m,
            products = new[] { new OrderProduct("Keyboard", 2, 20m), new OrderProduct("Mouse", 1, 10m) }
        });
        var orderId = (await created.Content.ReadFromJsonAsync<JsonElement>())
            .GetProperty("orderId").GetGuid();

        using var response = await client.GetAsync($"/orders/{orderId}");

        var products = (await response.Content.ReadFromJsonAsync<JsonElement>())
            .GetProperty("products").EnumerateArray().ToArray();
        Assert.Equal(2, products.Length);
        Assert.Equal("Keyboard", products[0].GetProperty("name").GetString());
        Assert.Equal(2, products[0].GetProperty("quantity").GetInt32());
        Assert.Equal(20m, products[0].GetProperty("unitPrice").GetDecimal());
    }

    [Fact]
    public async Task Get_unknown_order_returns_not_found()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();

        using var response = await client.GetAsync($"/orders/{Guid.NewGuid()}");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
    }

    [Fact]
    public async Task Status_update_for_unknown_order_returns_not_found_without_publishing()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();

        using var response = await client.PatchAsJsonAsync($"/orders/{Guid.NewGuid()}/status",
            new { status = "Completed" });

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Empty(app.Publisher.Statuses);
    }

    [Fact]
    public async Task Publish_failure_removes_the_persisted_order()
    {
        await using var app = new ApiFactory();
        app.Publisher.OnPublish = _ => throw new MessagePublishException("Not confirmed", new IOException("broker down"));
        using var client = app.CreateClient();

        using var response = await client.PostAsJsonAsync("/orders", new { customerName = "Sopanha", totalAmount = 50m });

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
        var attempted = Assert.Single(app.Publisher.Orders);
        using var get = await client.GetAsync($"/orders/{attempted.OrderId}");
        Assert.Equal(HttpStatusCode.NotFound, get.StatusCode);
    }

    [Fact]
    public async Task Fanout_endpoint_publishes_event_and_returns_accepted()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();

        using var response = await client.PostAsync("/routing/fanout", content: null);

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var routingEvent = Assert.Single(app.Publisher.FanoutEvents);
        Assert.NotEqual(Guid.Empty, routingEvent.EventId);
        Assert.Equal(OrderRoutingEventTypes.Created, routingEvent.EventType);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(routingEvent.EventId, body.GetProperty("eventId").GetGuid());
        Assert.Equal("orders.fanout", body.GetProperty("exchange").GetString());
    }

    [Fact]
    public async Task Topic_endpoint_trims_routing_key_and_publishes_event()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();

        using var response = await client.PostAsJsonAsync("/routing/topic",
            new { routingKey = " order.updated ", eventType = " OrderUpdated " });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var published = Assert.Single(app.Publisher.TopicEvents);
        Assert.Equal("order.updated", published.RoutingKey);
        Assert.Equal(OrderRoutingEventTypes.Updated, published.Event.EventType);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal("order.updated", body.GetProperty("routingKey").GetString());
        Assert.Equal("orders.topic", body.GetProperty("exchange").GetString());
    }

    [Fact]
    public async Task Topic_endpoint_defaults_to_order_created_event_type()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();

        using var response = await client.PostAsJsonAsync("/routing/topic", new { routingKey = "order.created" });

        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var published = Assert.Single(app.Publisher.TopicEvents);
        Assert.Equal(OrderRoutingEventTypes.Created, published.Event.EventType);
    }

    [Theory]
    [InlineData("", null)]
    [InlineData("   ", null)]
    [InlineData("order.created", "")]
    [InlineData("order.created", "   ")]
    public async Task Invalid_topic_request_returns_bad_request_without_publishing(string routingKey,
        string? eventType)
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();

        using var response = await client.PostAsJsonAsync("/routing/topic",
            new { routingKey, eventType });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.TopicEvents);
    }

    [Fact]
    public async Task Long_topic_routing_key_returns_bad_request_without_publishing()
    {
        await using var app = new ApiFactory();
        using var client = app.CreateClient();

        using var response = await client.PostAsJsonAsync("/routing/topic",
            new { routingKey = new string('x', 256) });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Empty(app.Publisher.TopicEvents);
    }

    [Fact]
    public async Task Fanout_publish_failure_returns_service_unavailable()
    {
        await using var app = new ApiFactory();
        app.Publisher.OnPublishFanout = _ =>
            throw new MessagePublishException("Not confirmed", new IOException("broker down"));
        using var client = app.CreateClient();

        using var response = await client.PostAsync("/routing/fanout", content: null);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    [Fact]
    public async Task Topic_publish_failure_returns_service_unavailable()
    {
        await using var app = new ApiFactory();
        app.Publisher.OnPublishTopic = _ =>
            throw new MessagePublishException("Not confirmed", new IOException("broker down"));
        using var client = app.CreateClient();

        using var response = await client.PostAsJsonAsync("/routing/topic",
            new { routingKey = "order.created" });

        Assert.Equal(HttpStatusCode.ServiceUnavailable, response.StatusCode);
    }

    private static async Task<Guid> CreateOrderAsync(HttpClient client)
    {
        using var response = await client.PostAsJsonAsync("/orders", new { customerName = "Sopanha", totalAmount = 50m });
        Assert.Equal(HttpStatusCode.Accepted, response.StatusCode);
        var body = await response.Content.ReadFromJsonAsync<JsonElement>();
        return body.GetProperty("orderId").GetGuid();
    }

    private sealed class ApiFactory : WebApplicationFactory<Program>
    {
        // Each factory gets its own database so tests do not see each other's orders.
        private readonly string _databasePath =
            Path.Combine(Path.GetTempPath(), $"order-messaging-tests-{Guid.NewGuid():N}.db");

        public FakePublisher Publisher { get; } = new();

        protected override void ConfigureWebHost(IWebHostBuilder builder) => builder.ConfigureServices(services =>
        {
            services.RemoveAll<IMessagePublisher>();
            services.AddSingleton<IMessagePublisher>(Publisher);

            services.RemoveAll<IDbContextFactory<OrderDbContext>>();
            services.RemoveAll<DbContextOptions<OrderDbContext>>();
            services.AddDbContextFactory<OrderDbContext>(options =>
                options.UseSqlite($"Data Source={_databasePath};Default Timeout=5"));
        });
    }

    private sealed class FakePublisher : IMessagePublisher
    {
        public List<OrderCreated> Orders { get; } = [];
        public List<OrderStatusChanged> Statuses { get; } = [];
        public List<OrderRoutingEvent> FanoutEvents { get; } = [];
        public List<(OrderRoutingEvent Event, string RoutingKey)> TopicEvents { get; } = [];
        public Func<CancellationToken, Task>? OnPublish { get; set; }
        public Func<CancellationToken, Task>? OnPublishStatus { get; set; }
        public Func<CancellationToken, Task>? OnPublishFanout { get; set; }
        public Func<CancellationToken, Task>? OnPublishTopic { get; set; }

        public async Task PublishAsync(OrderCreated order, CancellationToken cancellationToken)
        {
            Orders.Add(order);
            if (OnPublish is not null) await OnPublish(cancellationToken);
        }

        public async Task PublishStatusAsync(OrderStatusChanged status, CancellationToken cancellationToken)
        {
            Statuses.Add(status);
            if (OnPublishStatus is not null) await OnPublishStatus(cancellationToken);
        }

        public async Task PublishFanoutAsync(OrderRoutingEvent routingEvent, CancellationToken cancellationToken)
        {
            FanoutEvents.Add(routingEvent);
            if (OnPublishFanout is not null) await OnPublishFanout(cancellationToken);
        }

        public async Task PublishTopicAsync(OrderRoutingEvent routingEvent, string routingKey,
            CancellationToken cancellationToken)
        {
            TopicEvents.Add((routingEvent, routingKey));
            if (OnPublishTopic is not null) await OnPublishTopic(cancellationToken);
        }
    }
}
