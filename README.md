# How to Run Order Messaging System

This demo lets you send an order and watch it get processed.

Before starting, install **.NET 10 SDK**, **Docker with Docker Compose**, and **curl**. Make sure Docker is running.

Open a terminal in the `OrderMessagingSystem` folder. Use this folder in each new terminal below.

## 1. Build the project

Run each command and wait for it to finish:

```bash
dotnet restore OrderMessagingSystem.slnx
dotnet build OrderMessagingSystem.slnx --no-restore
```

You should see **Build succeeded**.

## 2. Start RabbitMQ

RabbitMQ holds orders until they are processed.

```bash
sudo docker compose up -d --wait
```

Enter your computer password if asked. Wait until it shows **Healthy**.

## 3. Start the order processor — terminal 1

In the same terminal, run:

```bash
dotnet run --project OrderConsumer.Worker
```

Wait for **Waiting for orders**. Leave this terminal running.

## 4. Start the API — terminal 2

The API receives the orders you send.

Open a **second terminal** in `OrderMessagingSystem` and run:

```bash
dotnet run --project OrderProducer.Api
```

Wait for **http://localhost:5000**. Leave this terminal running too.

## 5. Send an order — terminal 3

Open a **third terminal** and paste this whole command:

```bash
curl -i -X POST http://localhost:5000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerName":"Sopanha","totalAmount":50.00}'
```

You should receive **202 Accepted**. This means your order was received.

Check the **first terminal** for **Processed and acknowledged order**. Your project is working!

You can send another order by running the command again. Change `Sopanha` and `50.00` to try a different name and amount.

## 6. View the dashboard — optional

Open **http://localhost:15673** in your browser and sign in:

| Username | Password |
| --- | --- |
| `orderdev` | `orderdev_password` |

Select **Queues and Streams**, then **orders**. An empty queue is normal: the processor may have already finished the orders.

## 7. Stop when finished

Press **Ctrl+C** in the first and second terminals. Then run this in the third terminal:

```bash
sudo docker compose down
```

Your saved messages are kept.

To run the project again, start RabbitMQ with the command below, then repeat steps **3–5**:

```bash
sudo docker compose up -d --wait
```

## Need help?

If a step fails, copy the error shown in that terminal and include it when asking for help. Mention which step you were following.

## Consumer behavior

The worker processes one delivery at a time and acknowledges it only after `IOrderProcessor.ProcessAsync` completes. `OrderProcessor` currently simulates work with a one-second delay; replace that operation with your order fulfillment logic. Business operations should tolerate repeated deliveries of the same `OrderId`.

Malformed messages, invalid order values, and processing failures are rejected without requeueing and routed to `orders.dead-letter` (or `<QueueName>.dead-letter` when configured). Inspect that queue in the RabbitMQ dashboard when an order fails.

The worker reconnects after connection loss, channel closure, consumer cancellation, or acknowledgment failure, waiting five seconds between attempts. On shutdown it cancels unfinished work and leaves it unacknowledged for redelivery.

Run the API and consumer tests from `OrderMessagingSystem`:

```bash
dotnet test OrderMessagingSystem.slnx
```
