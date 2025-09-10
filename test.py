import json

def parse_block(lines, i=0):
    result = {}
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        if line.endswith("{") and "=>" not in line:  
            # début d'un bloc "normal" (ex: beats { ... })
            key = line.split()[0]
            sub_result, i = parse_block(lines, i + 1)
            result[key] = sub_result

        elif line == "}":  
            # fin de bloc
            return result, i + 1

        elif "=>" in line:
            key, value = map(str.strip, line.split("=>", 1))
            # si valeur est un sous-bloc inline : { ... }
            if value.startswith("{") and value.endswith("}"):
                inner = value[1:-1].strip()
                inner_lines = [l.strip() for l in inner.split(",") if l.strip()]
                inner_dict = {}
                for inner_line in inner_lines:
                    if "=>" in inner_line:
                        k, v = map(str.strip, inner_line.split("=>", 1))
                        v = v.strip('"').strip("'")
                        inner_dict[k.strip('"').strip("'")] = v
                result[key] = inner_dict
            else:
                value = value.strip('"').strip("'")
                if value.startswith("[") and value.endswith("]"):
                    value = [v.strip().strip('"').strip("'") for v in value[1:-1].split(",")]
                elif value.isdigit():
                    value = int(value)
                result[key] = value
            i += 1
        else:
            i += 1

    return result, i


def parse_logstash_pipeline(config: str) -> dict:
    lines = config.splitlines()
    result, _ = parse_block(lines, 0)
    return result


# Test
config_str = """
input {
  beats {
    port => 5044
  }
}
filter {
  grok {
    match => { "message" => "%{COMBINEDAPACHELOG}" }
  }
}
output {
  elasticsearch {
    hosts => ["localhost:9200"]
    index => "logs-%{+YYYY.MM.dd}"
  }
}
"""

parsed = parse_logstash_pipeline(config_str)
print(json.dumps(parsed, indent=2))
