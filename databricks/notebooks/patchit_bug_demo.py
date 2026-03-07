# Databricks notebook source
# Intentional bug for PATCHIT demo


def transform_records(records):
    total = 0
    for rec in records:
        total += rec['amount']
    return total / len(records)


if __name__ == '__main__':
    print(transform_records([{'amount': 4}, {'amount': 6}]))
