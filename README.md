# Crowd Cow Data Warehouse

## Docs

[Confluence](https://crowdcow.atlassian.net/wiki/spaces/ED/pages/170623021/Data+Engineering)

### Updating dbt Docs

This will be automated soon.

`dbt docs generate`

`aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 454711173051.dkr.ecr.us-east-1.amazonaws.com`

`docker build -t crowdcow-dbt-docs --build-arg DBT_DOCS_PASSWORD=<password> -f Dockerfile.dbtdocs`

`docker tag crowdcow-dbt-docs:latest 454711173051.dkr.ecr.us-east-1.amazonaws.com/crowdcow-dbt-docs:latest`

`docker push 454711173051.dkr.ecr.us-east-1.amazonaws.com/crowdcow-dbt-docs:latest`

Then update the `dbt-docs` app on Porter.

## Events

### Base Ahoy Event Model

Ahoy Events are parsed in the `models/staging/cc/base/base_cc__ahoy_events` model. All staging models are derived from this base model.

### Events in Need of Documentation

The directory `analysis/events_in_need_of_documentation` constains event models that are almost ready to be included, but the contents and naming need to be confirmed and documentation needs to be written.

## dbt

## Snowflake
