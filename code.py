import re
import json

def parse_block(block: str) -> dict:
    """Parse un bloc interne (plugin et ses paramètres)."""
    result = {}
    plugin_pattern = re.compile(r'(\w+)\s*{([^{}]*)}')
    for plugin, content in plugin_pattern.findall(block):
        plugin_dict = {}
        for line in content.strip().splitlines():
            line = line.strip()
            if "=>" in line:
                key, value = map(str.strip, line.split("=>", 1))
                value = value.strip().strip('"').strip("'")
                # Si liste
                if value.startswith("[") and value.endswith("]"):
                    items = [v.strip().strip('"').strip("'") for v in value[1:-1].split(",")]
                    value = items
                # Si nombre
                elif value.isdigit():
                    value = int(value)
                plugin_dict[key] = value
        result[plugin] = plugin_dict
    return result


def parse_logstash_pipeline(config: str) -> dict:
    """Parse une config Logstash complète en dictionnaire."""
    pipeline = {}
    section_pattern = re.compile(r'(input|filter|output)\s*{([^{}]*)}', re.DOTALL)
    for section, content in section_pattern.findall(config):
        pipeline[section] = parse_block(content)
    return pipeline


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