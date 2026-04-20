dbt test --migrations --target gold
        dbt test --migrations --target intermediate
        dbt test --migrations --target staging
