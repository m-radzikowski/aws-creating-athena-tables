# Creating Amazon Athena tables

Sample data flow AWS project presenting three different ways
for creating Athena tables.

Incoming data is mocked and randomly generated every minute.

You will be charged for the resources usage,
so **remember to tear down the stack** after you test it.

See the article with full description:
[Three ways to create Amazon Athena tables](https://betterdev.blog/creating-athena-tables/)

## Usage

Deploy stack. See below for instructions.

Glue job and Lambda functions will be triggered
automatically every minute.

Wait few minutes.

Go to Athena in AWS Console and check out existing tables:

- products
- transactions
- sales

Remove stack to not be charged for resources usage
(or at least disable the Glue job trigger and Lambda event).

## Development

Install dependencies:

```bash
yarn install
```

Deploy:

```bash
yarn run deploy --region REGION [--stage STAGE]
```

Remove deployed stack:

```bash
yarn run remove --region REGION [--stage STAGE]
```
