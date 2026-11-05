# Order Messaging System

This demo lets you send an order and watch it get processed.

It has three parts:

- **API:** receives your order.
- **RabbitMQ:** holds the order until it can be processed.
- **Consumer:** processes the order and shows a confirmation.

## Before you start

You need **.NET 10 SDK**, **Docker with Docker Compose**, and **curl** installed. Docker must be running.

Open a terminal in the `OrderMessagingSystem` folder. Use this folder in each terminal you open below.

## 1. Prepare the project

Run these commands one at a time:

```bash
dotnet restore OrderMessagingSystem.slnx

```
```bash 
dotnet build OrderMessagingSystem.slnx --no-restore
```

```bash 
sudo docker compose up -d --wait
```
Wait for **Build succeeded** and for RabbitMQ to show **Healthy**. The first start may take a few minutes.

## 2. Start the order processor

In the same terminal, run:

```bash
dotnet run --project OrderConsumer.Worker
```

When you see **Waiting for orders**, it is ready. Leave this terminal open.

## 3. Start the API

Open a **second terminal** in the same folder and run:

```bash
dotnet run --project OrderProducer.Api
```

When you see **http://localhost:5000**, it is ready. Leave this terminal open too.

## 4. Send an order

Open a **third terminal** in the same folder. Copy and run this whole command:

```bash
curl -i -X POST http://localhost:5000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerName":"Sopanha","totalAmount":50.00}'
```

You can change `Sopanha` and `50.00` to try another name and amount.

Look for **202 Accepted**. This means your order was received.

Now look at the **first terminal**. When you see **Processed and acknowledged order**, your order is finished.

## View orders in your browser — optional

Open **http://localhost:15673** and sign in:

| Username | Password |
| --- | --- |
| `orderdev` | `orderdev_password` |

Select **Queues and Streams**, then **orders**. An empty queue is normal: the processor may have already finished the orders.

## Stop the project

Press **Ctrl+C** in the first and second terminals. Then run this in the third terminal:

```bash
sudo docker compose down
```

Your saved messages are kept. To run the demo again, start RabbitMQ with the command below, then repeat steps **2–4**:

```bash
sudo docker compose up -d --wait
```

## Something went wrong?

| What you see | What to do |
| --- | --- |
| `dotnet` or `docker` command not found | Install the missing tool before starting. |
| Cannot connect to the API | Check that the second terminal is still running and shows `http://localhost:5000`. |
| `400` response | Use the example order above. The name must not be empty and the amount must be greater than zero. |
| Login refused, `ACCESS_REFUSED`, or `503` | Run the command below, then restart both apps. |

```bash
sudo docker compose up -d --force-recreate --wait
```

If the problem continues, copy the error from the terminal when asking for help.
