# Confluent Cloud Registry Action

![Master Workflow](https://github.com/DivLoic/ccloud-registry-action/workflows/Master%20Workflow/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/DivLoic/ccloud-registry-action/badge.svg?branch=master)](https://coveralls.io/github/DivLoic/ccloud-registry-action?branch=master)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FDivLoic%2Fccloud-registry-action.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2FDivLoic%2Fccloud-registry-action?ref=badge_shield)
[![License](http://img.shields.io/:license-Apache%202-blue.svg)](LICENSE)

This Github Action validates the compatibility of your avro schemas against 
[Confluent Cloud](https://www.confluent.io/confluent-cloud) 
[Schema Registry](https://github.com/confluentinc/schema-registry). 
Regard less of your programming language you can use it just by creating the two following files.

An `schema.yml` mapping schemas to subjects for validation
```yaml
---
schemas:
  - subject: subject1-key
    file: schema0.avsc
# - ...
```

A Github Actions specification in the workflow folder (`.github/workflows/*.yml`)
```yaml
uses: divloic/ccloud-registry-action@v0.0.1
with:
  avro-files-path: src/main/avro/
  avro-subject-yaml: schemas.yaml
  schema-registry-url: ${{ secrets.SCHEMA_REGISTRY_URL }}
  schema-registry-api-key: ${{ secrets.SCHEMA_REGISTRY_API_KEY }}
  schema-registry-secret-key: ${{ secrets.SCHEMA_REGISTRY_SECRET_KEY }}
```

## Inputs

input                           | description                   | default
--------------------------------|-------------------------------|------------------
`avro-files-path`               | path to the schemas folder    | `src/main/avro/`
`avro-subject-yaml`             | path to the subject file      | `/schema.yml`
`schema-registry-url`           | url to the schema-registry    | ⚠️ *required*
`schema-registry-api-key`       | Confluent API key             | ⚠️ *required*
`schema-registry-secret-key`    | Confluent secret key          | ⚠️ *required*

## Outputs

This action has no output yet. It simply logs or crashes in case of incompatibilities.

## Example usage

You can find a complete example in this repository: 
[ccloud-registry-example](https://github.com/DivLoic/ccloud-registry-example/)

## Contribute

- Source Code: [https://github.com/DivLoic/ccloud-registry-action/](https://github.com/DivLoic/ccloud-registry-action/)
- Issue Tracker: [https://github.com/DivLoic/ccloud-registry-action/issues](https://github.com/DivLoic/ccloud-registry-action/issues)

## Licence

This project is licensed under [Apache License 2.0](LICENSE)

[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2FDivLoic%2Fccloud-registry-action.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2FDivLoic%2Fccloud-registry-action?ref=badge_large)
