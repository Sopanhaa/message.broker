# OrderMessagingSystem

This project accepts an order through an API and sends it to RabbitMQ. A worker receives the order and prints the processing result in its terminal.

```text
curl / Postman → OrderProducer.Api → RabbitMQ → OrderConsumer.Worker
```

RabbitMQ runs in Docker. The API and worker run locally with `dotnet run`.

## 1. Check the required tools

You need .NET 10 SDK, Docker with Docker Compose, and curl (or Postman).

```bash
dotnet --version
docker --version
docker compose version
```

The .NET SDK version should start with `10.`. If a tool is missing, see the [.NET installation guide](https://learn.microsoft.com/en-us/dotnet/core/install/linux-ubuntu-install) or [Docker installation guide](https://docs.docker.com/engine/install/ubuntu/).

## 2. Open the project folder

From the repository folder (`message.broker`), run:

```bash
cd OrderMessagingSystem
```

You should see `OrderMessagingSystem.slnx` and `docker-compose.yml` in this folder. Run all commands below from this folder, including commands in new terminals.

## 3. Restore and build

```bash
dotnet restore OrderMessagingSystem.slnx
dotnet build OrderMessagingSystem.slnx --no-restore
```

Continue when you see `Build succeeded`.

## 4. Start RabbitMQ

```bash
sudo docker compose up -d --wait
sudo docker compose ps
```

The first start downloads RabbitMQ and may take a few minutes. Wait until the container shows `healthy`.

If your account already has Docker access, you can omit `sudo`.

The project already includes matching development connection settings in both applications' `appsettings.json` files:

| Setting | Value |
| --- | --- |
| Host | `localhost` |
| AMQP port | `5672` |
| Username | `orderdev` |
| Password | `orderdev_password` |
| Queue | `orders` |

You do not need to change these settings for the default setup.

## 5. Start the consumer

In **terminal 1**, run:

```bash
dotnet run --project OrderConsumer.Worker
```

Wait for a log message starting with `Waiting for orders in orders`. Leave this terminal running.

## 6. Start the API

Open **terminal 2** in the same project folder and run:

```bash
dotnet run --project OrderProducer.Api
```

The API starts at **http://localhost:5000**. Leave this terminal running too.

## 7. Send an order

Open **terminal 3** and run:

```bash
curl -i http://localhost:5000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerName":"Sopanha","totalAmount":50.00}'
```

For Postman, use **POST**, URL `http://localhost:5000/orders`, and **Body → raw → JSON** with the same JSON data.

You should receive **202 Accepted** and a generated `orderId`. This means RabbitMQ accepted the message for processing.

Look at **terminal 1** for output similar to:

```text
Processing order <orderId>: customer=Sopanha, total=50.00
Processed and acknowledged order <orderId>
```

The customer name must contain 1–100 characters after trimming, and the amount must be greater than zero. Invalid data returns `400`.

## 8. Open the RabbitMQ dashboard

Open **http://localhost:15672** in your browser.

- Username: `orderdev`
- Password: `orderdev_password`

Go to **Queues and Streams** to see `orders` and `orders.dead-letter`. Failed messages are sent to `orders.dead-letter`.

To see messages waiting in `orders`, stop the consumer with `Ctrl+C`, send another order, and refresh the dashboard. Start the consumer again to process the waiting message.

## 9. Run tests (optional)

Run the API tests:

```bash
dotnet test OrderMessagingSystem.slnx
```

## 10. Stop the project

Press `Ctrl+C` in the API and consumer terminals. Then stop RabbitMQ:

```bash
sudo docker compose down
```

The RabbitMQ data volume is kept, so stored messages remain available the next time you run `sudo docker compose up -d --wait`.

## Common setup problems

### Docker: permission denied

Use `sudo` before the Docker command:

```bash
sudo docker compose up -d --wait
```

### Port 5672 is already in use

Another service or container is using RabbitMQ's port. Find it:

```bash
sudo ss -ltnp 'sport = :5672'
sudo docker ps --filter publish=5672
```

If it is an old container you no longer need, stop it using its name or ID:

```bash
sudo docker stop <container-name-or-id>
```

If it is RabbitMQ installed directly on Ubuntu and you want to use this project's container instead:

```bash
sudo systemctl stop rabbitmq-server
```

Then retry `sudo docker compose up -d --wait`. Stop the existing service only if it is safe to do so.

### API returns 503 or the worker cannot connect

Check that RabbitMQ is running and healthy:

```bash
sudo docker compose ps
sudo docker compose logs --tail 50 rabbitmq
```

Confirm that both applications' `appsettings.json` files use the connection settings shown in step 4.
