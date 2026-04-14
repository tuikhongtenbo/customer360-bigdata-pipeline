from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DQRule:
    def __init__(self, name, predicate_col):
        self.name = name
        self.predicate_col = predicate_col


def not_null(col_name):
    predicate_col = col(col_name).isNotNull()
    return DQRule(f"not_null_{col_name}", predicate_col)


def non_negative(col_name):
    predicate_col = col(col_name) >= 0
    return DQRule(f"non_negative_{col_name}", predicate_col)


def evaluate(df: DataFrame, rules: list, max_rejection_rate: float = 0.05):
    valid_mask = None
    for rule in rules:
        valid_mask = rule.predicate_col if valid_mask is None else valid_mask & rule.predicate_col
    
    valid_df = df.filter(valid_mask)
    invalid_df = df.filter(~valid_mask)
    
    total = df.count()
    invalid_n = invalid_df.count()
    rate = invalid_n / total if total > 0 else 0.0
    
    report = {
        "total": total,
        "valid": total - invalid_n,
        "invalid": invalid_n,
        "rate": rate,
        "exceeded": rate > max_rejection_rate
    }
    return valid_df, invalid_df, report