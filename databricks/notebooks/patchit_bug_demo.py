# Databricks notebook source
# Intentional bug for PATCHIT demo


def transform_records(records):
    total = 0
    for rec in records:
        total += rec['amount']
    # Typo bug: average_count is undefined
    return total / average_count


if __name__ == '__main__':
    print(transform_records([{'amount': 4}, {'amount': 6}]))
