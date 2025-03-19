
rules = [
    {
      "name": "raw_contain_all_required_cdc_fields",
      "constraint": "op IS NOT NULL AND ts IS NOT NULL AND AFTER IS NOT NULL AND SOURCE IS NOT NULL",
      "tag": "validity"
    }
  ]

def get_rules_by_tag(tag: str) -> list[str]:
  return [rule['constraint'] for rule in rules if rule['tag'] == tag]

def get_all_rules() -> list[dict]:
  return rules
