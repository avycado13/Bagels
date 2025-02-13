# Contributing migrations

If you have a migration script you'd like to share, please submit a pull request to this repository! Your script should be placed in a subdirectory under `src/bagels/migrations/` and also update this `MIGRATION.md` file with instructions on how to migrate.

**IMPORTANT**: Running migrations on an existing Bagels database may have unexpected results. Always backup your Bagels database before running the migration.

# Actual Budget to Bagels Migration

Migrate data from Actual Budget (https://actualbudget.com/) to Bagels. It transfers your accounts, categories, and transactions while preserving their relationships and attributes.

## Rules

- Transactions without categories in Actual Budget will be assigned to an "Uncategorized" category in Bagels
- The script converts all amounts from cents (Actual Budget) to dollars (Bagels)

## Steps to Migrate

1. Export Your Actual Budget Data:

- Open Actual Budget
- Navigate to More → Settings → Export Data
- Save and extract the zip file
- Locate the `db.sqlite` file in the extracted contents

2. Run the Migration:

```bash
bagels --migrate actualbudget --source "path/to/actualbudget/db.sqlite"
```

# Ledger to Bagels Migration

Migrate data from Ledger (<https://ledger-cli.org>) to Bagels. It transfers your accounts, categories, and transactions while preserving their relationships and attributes.

## Rules

- Transactions without categories in Ledger will be assigned to an "Uncategorized" category in Bagels

## Steps to Migrate

1. Export Your Ledger Data:

- Open Ledger
- Navigate to More → Settings → Export Data
- Save and extract the zip file
- Locate the `db.sqlite` file in the extracted contents

2. Run the Migration:

```bash
bagels --migrate ledger --source "path/to/ledger/csv"
```
