import csv
import sqlite3
from datetime import datetime


class LedgerToBagelsMigration:
    def __init__(self, hledger_csv: str, bagels_db: str):
        self.hledger_csv = hledger_csv
        self.bagels_conn = sqlite3.connect(bagels_db)
        self.bagels_cur = self.bagels_conn.cursor()
        self.txn_map = {}

    def migrate_accounts(self):
        """Populates the account table with distinct account names from hledger CSV."""
        accounts = set()

        with open(self.hledger_csv, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                accounts.add(row["account"])

        for account in accounts:
            self.bagels_cur.execute(
                """
                INSERT OR IGNORE INTO account (
                    createdAt, updatedAt, name, description, 
                    beginningBalance, hidden
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (datetime.now(), datetime.now(), account, None, 0.0, 0),
            )

    def get_account_id(self, account: str) -> int:
        """Maps hledger account names to Bagels account IDs."""
        self.bagels_cur.execute("SELECT id FROM account WHERE name = ?", (account,))
        result = self.bagels_cur.fetchone()
        if result:
            return result[0]
        raise ValueError(f"Account '{account}' not found in Bagels database.")

    def migrate_splits(self):
        """Insert postings into the split table."""
        with open(self.hledger_csv, newline="") as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                txnidx = row["txnidx"]
                account = row["account"]
                amount = float(row["amount"])
                account_id = self.get_account_id(account)

                self.bagels_cur.execute(
                    """
                    INSERT INTO split (
                        createdAt, updatedAt, recordId, amount, 
                        personId, isPaid, paidDate, accountId
                    )
                    VALUES (?, ?, ?, ?, NULL, 0, NULL, ?)
                    """,
                    (datetime.now(), datetime.now(), txnidx, amount, account_id),
                )

                split_id = self.bagels_cur.lastrowid
                self.txn_map.setdefault(txnidx, []).append(split_id)

    def migrate_records(self):
        """Insert transactions into the record table."""
        for txnidx, split_ids in self.txn_map.items():
            # Aggregate splits for the transaction
            placeholders = ",".join("?" for _ in split_ids)
            self.bagels_cur.execute(
                f"SELECT SUM(amount) FROM split WHERE id IN ({placeholders})", split_ids
            )
            total_amount = self.bagels_cur.fetchone()[0]

            # Get transaction details
            self.bagels_cur.execute(
                """
                SELECT description, date1 FROM postings 
                WHERE txnidx = ? LIMIT 1
                """,
                (txnidx,),
            )
            description, date1 = self.bagels_cur.fetchone()

            # Insert record
            self.bagels_cur.execute(
                """
                INSERT INTO record (
                    createdAt, updatedAt, label, amount, date, 
                    accountId, isIncome, isTransfer
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    datetime.now(),
                    datetime.now(),
                    description,
                    total_amount,
                    date1,
                    None,  # Default accountId
                    1 if total_amount > 0 else 0,
                    0,  # Assuming no transfers
                ),
            )

            record_id = self.bagels_cur.lastrowid

            # Update splits with record ID
            self.bagels_cur.executemany(
                "UPDATE split SET recordId = ? WHERE id = ?",
                [(record_id, split_id) for split_id in split_ids],
            )

    def migrate(self):
        """Execute the full migration process."""
        self.bagels_conn.execute("BEGIN TRANSACTION")
        try:
            self.migrate_accounts()
            self.migrate_splits()
            self.migrate_records()
            self.bagels_conn.commit()
            print("Migration completed successfully!")
        except Exception as e:
            self.bagels_conn.rollback()
            raise e
        finally:
            self.bagels_conn.close()


# if __name__ == "__main__":
#     # Example usage:
#     # curl -O https://raw.githubusercontent.com/simonmichael/hledger/master/examples/sample.journal
#     # hledger -f sample.journal print -O csv > hledger.csv

#     hledger_csv = "hledger.csv"
#     bagels_db = "/Users/simon/.local/share/bagels/db.db"

#     migrator = LedgerToBagelsMigration(hledger_csv, bagels_db)
#     migrator.migrate()
