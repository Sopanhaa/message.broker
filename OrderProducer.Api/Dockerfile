# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Restore from the project files first so code changes reuse the NuGet layer.
COPY Shared.Contracts/Shared.Contracts.csproj Shared.Contracts/
COPY OrderPersistence/OrderPersistence.csproj OrderPersistence/
COPY OrderProducer.Api/OrderProducer.Api.csproj OrderProducer.Api/
RUN dotnet restore OrderProducer.Api/OrderProducer.Api.csproj

COPY Shared.Contracts/ Shared.Contracts/
COPY OrderPersistence/ OrderPersistence/
COPY OrderProducer.Api/ OrderProducer.Api/
RUN dotnet publish OrderProducer.Api/OrderProducer.Api.csproj \
    --configuration Release \
    --no-restore \
    --output /app/publish \
    /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS final
WORKDIR /app

ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080

# The API and worker share this directory through a Docker volume.
RUN mkdir -p /data && chown "$APP_UID":"$APP_UID" /data

COPY --from=build /app/publish .
USER $APP_UID

ENTRYPOINT ["dotnet", "OrderProducer.Api.dll"]
