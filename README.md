# OrderMessagingSystem — Setup Guide

Follow these steps to run the project on Ubuntu.

The project has three parts:

- **RabbitMQ** stores order messages and runs in Docker.
- **OrderProducer.Api** receives HTTP requests and sends messages to RabbitMQ.
- **OrderConsumer.Worker** receives messages and logs the processed orders.

## 1. Check your tools

Install the **.NET 10 SDK**, **Docker with Docker Compose**, and **curl** before continuing.

Installation guides: [.NET on Ubuntu](https://learn.microsoft.com/en-us/dotnet/core/install/linux-ubuntu-install) · [Docker on Ubuntu](https://docs.docker.com/engine/install/ubuntu/).

Check that they are available:

```bash
dotnet --version
docker --version
docker compose version
curl --version
```

The .NET version should start with `10.`.

## 2. Open the solution folder

Open a terminal in the folder containing `OrderMessagingSystem.slnx` and `docker-compose.yml`.

If you are in the parent `message.broker` folder, run:

```bash
cd OrderMessagingSystem
```

If your terminal is already in `OrderMessagingSystem`, stay there. Use this folder for all commands below and for each new terminal.

## 3. Build the project

```bash
dotnet restore OrderMessagingSystem.slnx
dotnet build OrderMessagingSystem.slnx --no-restore
```

Wait for `Build succeeded` before continuing.

## 4. Start RabbitMQ

```bash
sudo docker compose up -d --wait
sudo docker compose ps
```

The first run downloads the RabbitMQ image. Wait until the container status shows `healthy`.

You can omit `sudo` if your account already has Docker access.

Open **http://localhost:15672** and log in:

| Username | Password |
| --- | --- |
| `orderdev` | `orderdev_password` |

Both applications are already configured to connect to `localhost:5672` with these development credentials. No configuration changes are needed for this setup.

## 5. Run the consumer — terminal 1

```bash
dotnet run --project OrderConsumer.Worker
```

Wait for `Waiting for orders in orders`. Keep this terminal running.

## 6. Run the API — terminal 2

Open another terminal in the solution folder:

```bash
dotnet run --project OrderProducer.Api
```

Keep it running. The API URL is **http://localhost:5000**.

## 7. Send an order — terminal 3

Check that the API is running:

```bash
curl http://localhost:5000/health
```

Send an order:

```bash
curl -i -X POST http://localhost:5000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerName":"Sopanha","totalAmount":50.00}'
```

You should receive **HTTP 202 Accepted**, with an `orderId` in the response. This means RabbitMQ accepted the message.

In **terminal 1**, you should see:

```text
Processing order <orderId>: customer=Sopanha, total=50.00
Processed and acknowledged order <orderId>
```

You can also send the request using Postman: choose **POST**, enter `http://localhost:5000/orders`, select **Body → raw → JSON**, and use:

```json
{
  "customerName": "Sopanha",
  "totalAmount": 50.00
}
```

## 8. Check the queue

In the RabbitMQ dashboard, open **Queues and Streams → orders**.

To see a message waiting:

1. Stop the consumer in terminal 1 with `Ctrl+C`.
2. Send another order using the same curl command.
3. Refresh the dashboard. The queue should show a ready message.
4. Run `dotnet run --project OrderConsumer.Worker` again. The worker processes the waiting message.

## 9. Run tests (optional)

From the solution folder:

```bash
dotnet test OrderMessagingSystem.slnx
```

These API tests can run without RabbitMQ.

## 10. Stop the project

Press `Ctrl+C` in the API and consumer terminals. Then run:

```bash
sudo docker compose down
```

This keeps RabbitMQ's stored data. To start the project again, repeat steps 4–6.

## Setup problems

**Docker says permission denied:** use `sudo docker compose up -d --wait`.

**Port 5672 is already in use:** check which service or container owns it:

```bash
sudo ss -ltnp 'sport = :5672'
sudo docker ps --filter publish=5672
```

If an old container is using it and you no longer need that container, stop it with `sudo docker stop CONTAINER_NAME`, replacing `CONTAINER_NAME` with the name shown above.

If RabbitMQ is installed directly on Ubuntu and you want to replace it with this project's container, stop that service with `sudo systemctl stop rabbitmq-server`. Stop it only if other applications do not need it.

Then retry `sudo docker compose up -d --wait`.

**The API returns 503 or the consumer cannot connect:** check RabbitMQ:

```bash
sudo docker compose ps
sudo docker compose logs --tail 50 rabbitmq
```

Check that `OrderProducer.Api/appsettings.json` and `OrderConsumer.Worker/appsettings.json` both use host `localhost`, port `5672`, username `orderdev`, and password `orderdev_password`.
