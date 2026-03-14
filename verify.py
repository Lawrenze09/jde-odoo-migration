from extractors.mock_extractor import MockExtractor
from transformers.customer_transformer import CustomerTransformer
from validators.customer_validator import CustomerValidator

records = MockExtractor().extract()
transformed = CustomerTransformer().transform_batch(records)
valid, failed = CustomerValidator().validate_batch(transformed)

print(f"Valid: {len(valid)}")
print(f"Failed: {len(failed)}")
print()
for f in failed:
    print(f"AN8={f['_jde_an8']} | {f['_failed_rule']} | {f['_failure_reason']}")
    