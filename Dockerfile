FROM microsoft/dotnet:2.2.100-sdk AS build  
WORKDIR /app

ARG VERSION_SUFFIX=0-dev
ENV VERSION_SUFFIX=$VERSION_SUFFIX

COPY ./*.sln ./NuGet.config ./
COPY ./build/*.props ./build/

# Copy the main source project files
COPY src/*/*.csproj ./
RUN for file in $(ls *.csproj); do mkdir -p src/${file%.*}/ && mv $file src/${file%.*}/; done

# Copy the test project files
COPY tests/*/*.csproj ./
RUN for file in $(ls *.csproj); do mkdir -p tests/${file%.*}/ && mv $file tests/${file%.*}/; done

RUN dotnet restore

# Copy everything else and build
COPY . .
RUN dotnet build --version-suffix $VERSION_SUFFIX -c Release

# testrunner

FROM build AS testrunner
WORKDIR /app/tests/Foundatio.AzureStorage.Tests
ENTRYPOINT dotnet test --results-directory /app/artifacts --logger:trx

# pack

FROM build AS pack
WORKDIR /app/

ARG VERSION_SUFFIX=0-dev
ENV VERSION_SUFFIX=$VERSION_SUFFIX

ENTRYPOINT dotnet pack --version-suffix $VERSION_SUFFIX -c Release -o /app/artifacts

# publish

FROM pack AS publish
WORKDIR /app/

ENTRYPOINT [ "dotnet", "nuget", "push", "/app/artifacts/*.nupkg" ]