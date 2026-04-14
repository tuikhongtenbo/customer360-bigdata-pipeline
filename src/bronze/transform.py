import sys
sys.path.insert(0, "/opt/airflow")
from pyspark.sql import DataFrame
from src.dq.evaluate import not_null, non_negative, evaluate


def transform(df):
    df = df.dropDuplicates(["_id"])

    print(f"[DQ DEBUG] Total rows: {df.count()}")

    for i, rule in enumerate([
            not_null("contract"),
            not_null("mac"), 
            not_null("total_duration"),
            non_negative("total_duration"),
        ]):
            mask = rule.predicate_col  
            valid_count = df.filter(mask).count()
            print(f"[DQ DEBUG] Rule {i}: {valid_count}/{df.count()} valid ({valid_count/df.count()*100:.2f}%)")

    rules = [
        not_null("contract"),
        not_null("mac"),
        not_null("total_duration"),
        non_negative("total_duration"),
    ]
    valid, invalid, report = evaluate(df, rules)
    
    if report["exceeded"]:
        raise RuntimeError(f"DQ rate {report['rate']:.2%} > 5%")
    return valid, invalid, report