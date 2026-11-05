# Order Messaging System

Send an order to an API and watch a background worker process it through RabbitMQ. This guide walks you through running the project locally on Ubuntu.

```text
curl or Postman → OrderProducer.Api → RabbitMQ → OrderConsumer.Worker
                 receives the order  queues it  processes and logs it
```

## Before you start

You need **.NET 10 SDK**, **Docker with Docker Compose**, and **curl**.

Check your tools:

```bash
dotnet --version
docker --version
docker compose version
curl --version
```

The .NET version should start with `10.`. If something is missing, follow the [.NET installation guide](https://learn.microsoft.com/en-us/dotnet/core/install/linux-ubuntu-install) or the [Docker installation guide](https://docs.docker.com/engine/install/ubuntu/).

Open a terminal in the `OrderMessagingSystem` folder. If you are in the parent `message.broker` folder, run:

```bash
cd OrderMessagingSystem
```

You should see `OrderMessagingSystem.slnx` and `docker-compose.yml` here. **Run every command below from this folder**, including commands in new terminals.

## 1. Build the project

```bash
dotnet restore OrderMessagingSystem.slnx
dotnet build OrderMessagingSystem.slnx --no-restore
```

Wait for **Build succeeded** before continuing.

## 2. Start RabbitMQ

```bash
sudo docker compose up -d --wait
sudo docker compose ps
```

The first run downloads the RabbitMQ image and may take a little longer. Continue when the container shows **healthy**.

These instructions use `sudo` for Docker commands. If your account already has access to the system Docker daemon, you can omit it. Use the same Docker context throughout; see [Docker permission or context problems](#docker-permission-or-context-problems) if containers appear to be missing.

Your local connection details are:

| Service | Address | Purpose |
| --- | --- | --- |
| Order API | http://localhost:5000 | Send orders and check the API |
| RabbitMQ dashboard | http://localhost:15673 | View queues and messages |
| RabbitMQ connection | `localhost:5673` | Used by the API and worker |

Open the **[RabbitMQ dashboard](http://localhost:15673)** and sign in with username **`orderdev`** and password **`orderdev_password`**. These are development credentials.

Both apps are already configured with these settings. The project uses host ports `5673` and `15673` to avoid conflicts with other RabbitMQ installations, including OnlyOffice.

## 3. Start the consumer — terminal 1

```bash
dotnet run --project OrderConsumer.Worker
```

Wait for a log line that starts with:

```text
Waiting for orders in orders
```

**Leave this terminal running.** The consumer waits for messages and logs each order it processes.

## 4. Start the API — terminal 2

Open a second terminal in `OrderMessagingSystem`:

```bash
dotnet run --project OrderProducer.Api
```

Wait for:

```text
Now listening on: http://localhost:5000
```

**Leave this terminal running too.**

## 5. Send your first order — terminal 3

Open a third terminal in `OrderMessagingSystem`. First, check the API:

```bash
curl http://localhost:5000/health
```

Expected response:

```json
{"status":"healthy","check":"liveness"}
```

This confirms the API is running. Sending an order in the next step checks its connection to RabbitMQ.

```bash
curl -i -X POST http://localhost:5000/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerName":"Sopanha","totalAmount":50.00}'
```

Look for **HTTP 202 Accepted** and a response like this; the ID and timestamp will be different:

```json
{
  "orderId": "1f53a5c9-12c0-409d-b051-ec261572a471",
  "status": "accepted",
  "createdAtUtc": "2026-11-05T03:26:27.4582817+00:00"
}
```

`202` means RabbitMQ accepted the order. Check **terminal 1** to confirm the consumer finished processing it:

```text
Processing order <orderId>: customer=Sopanha, total=50.00
Processed and acknowledged order <orderId>
```

The displayed amount may be `50` or `50.00`.

### Prefer Postman?

1. Create a **POST** request to `http://localhost:5000/orders`.
2. Select **Body → raw → JSON**.
3. Paste the following body and click **Send**:

```json
{
  "customerName": "Sopanha",
  "totalAmount": 50.00
}
```

Check for **202 Accepted**, then look at the consumer terminal for the processing logs.

## See messages in the dashboard

Open the [RabbitMQ dashboard](http://localhost:15673), then select **Queues and Streams → orders**. The queue appears after the consumer connects or the API publishes its first order.

The consumer processes messages quickly, so an empty queue after sending an order is normal. To watch a message wait in the queue:

1. Stop the consumer in terminal 1 with **Ctrl+C**. Keep the API running.
2. Send another order from terminal 3.
3. Refresh the dashboard. The `orders` queue should show a **Ready** message.
4. Start the consumer again with `dotnet run --project OrderConsumer.Worker`.
5. Watch the consumer logs. The message disappears from the queue after it is processed and acknowledged.

## Stop and restart

To stop, press **Ctrl+C** in the API and consumer terminals, then run:

```bash
sudo docker compose down
```

This keeps RabbitMQ's stored data, including queued messages.

Next time, start RabbitMQ:

```bash
sudo docker compose up -d --wait
```

Then run each app in its own terminal:

| Terminal | Command |
| --- | --- |
| 1 — Consumer | `dotnet run --project OrderConsumer.Worker` |
| 2 — API | `dotnet run --project OrderProducer.Api` |
| 3 — Requests | Use the curl commands in step 5 |

## Run the automated tests

```bash
dotnet test OrderMessagingSystem.slnx
```

These API tests can run without RabbitMQ.

## Troubleshooting

### The API does not respond

Make sure terminal 2 is still running and shows `Now listening on: http://localhost:5000`. Use `http`, and check that the URL includes port `5000`.

### The API returns 400

Check your JSON. Use a nonempty `customerName` of at most 100 characters and a `totalAmount` greater than zero. Include the `Content-Type: application/json` header, as shown in step 5.

### The API returns 503 or the consumer keeps reconnecting

Check the broker's status and recent logs:

```bash
sudo docker compose ps
sudo docker compose logs --tail 50 rabbitmq
```

Check the `RabbitMq` section in both [API settings](OrderProducer.Api/appsettings.json) and [consumer settings](OrderConsumer.Worker/appsettings.json):

| Setting | Expected value |
| --- | --- |
| `HostName` | `localhost` |
| `Port` | `5673` |
| `UserName` | `orderdev` |
| `Password` | `orderdev_password` |
| `VirtualHost` | `/` |

Restart the apps after changing their settings. Environment variables such as `RabbitMq__Port`, `RabbitMq__UserName`, and `RabbitMq__Password` override these files.

### Login refused / ACCESS_REFUSED

The app reached a RabbitMQ broker, but that broker rejected its credentials. Make sure it is connecting to this project's broker:

```bash
sudo docker compose port rabbitmq 5672
```

Expected output: `127.0.0.1:5673`. Here, `5672` is the port **inside the container**; the apps connect to host port `5673`.

If you recently changed the port mappings, recreate the container:

```bash
sudo docker compose up -d --force-recreate --wait
```

Then verify the development credentials inside the container:

```bash
sudo docker compose exec rabbitmq rabbitmqctl authenticate_user orderdev orderdev_password
```

If this prints **Success**, recheck the app settings above. If it fails, inspect the existing users and broker logs:

```bash
sudo docker compose exec rabbitmq rabbitmqctl list_users
sudo docker compose logs --tail 50 rabbitmq
```

RabbitMQ creates the default user when it first creates its database. Changing the default credentials in Compose does not update users already stored in the data volume. See [RabbitMQ's access control documentation](https://www.rabbitmq.com/docs/access-control) for user management.

### Docker permission or context problems

For a permission error, use the `sudo docker compose ...` commands shown in this guide.

If commands with and without `sudo` show different containers, they may be using different Docker daemons. Check the contexts:

```bash
docker context show
sudo docker context show
```

Use the same context for all Docker commands. If your account has access to the system daemon, you can explicitly select it with `docker --context default compose ...`.

### A port is already in use

Check which process is listening on the affected port:

```bash
sudo ss -ltnp '( sport = :5000 or sport = :5673 or sport = :15673 )'
```

If you already have this project's API running, use that instance or stop it with **Ctrl+C** before starting another.

If you change RabbitMQ's host port in [docker-compose.yml](docker-compose.yml), update `RabbitMq:Port` in both app settings files to match, recreate the container, and restart the apps.
