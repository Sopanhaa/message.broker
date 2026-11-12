# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore from the project files first so code changes reuse the NuGet layer.
COPY Shared.Contracts/Shared.Contracts.csproj Shared.Contracts/
COPY OrderPersistence/OrderPersistence.csproj OrderPersistence/
COPY OrderConsumer.Worker/OrderConsumer.Worker.csproj OrderConsumer.Worker/
RUN dotnet restore OrderConsumer.Worker/OrderConsumer.Worker.csproj

COPY Shared.Contracts/ Shared.Contracts/
COPY OrderPersistence/ OrderPersistence/
COPY OrderConsumer.Worker/ OrderConsumer.Worker/
RUN dotnet publish OrderConsumer.Worker/OrderConsumer.Worker.csproj \
    --configuration Release \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app

# The API and worker share this directory through a Docker volume.
RUN mkdir -p /data && chown "$APP_UID":"$APP_UID" /data

COPY --from=build /app/publish .
USER $APP_UID

ENTRYPOINT ["dotnet", "OrderConsumer.Worker.dll"]
