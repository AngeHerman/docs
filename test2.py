import re
import json

def strip_comments_from_line(line):
    """Retire la portion après # sauf si # est dans une chaîne."""
    res = []
    in_s = False  # inside single quotes
    in_d = False  # inside double quotes
    esc = False
    for ch in line:
        if esc:
            res.append(ch)
            esc = False
            continue
        if ch == '\\\\':
            res.append(ch)
            esc = True
            continue
        if ch == "'" and not in_d:
            in_s = not in_s
            res.append(ch)
            continue
        if ch == '"' and not in_s:
            in_d = not in_d
            res.append(ch)
            continue
        if ch == '#' and not in_s and not in_d:
            break  # début d'un commentaire réel
        res.append(ch)
    return ''.join(res)

def remove_comments(config):
    return '\n'.join(strip_comments_from_line(l) for l in config.splitlines())

def parse_block(lines, i=0):
    result = {}
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue

        if line.endswith("{") and "=>" not in line:
            key = line.split()[0]
            sub_result, i = parse_block(lines, i + 1)
            # gérer clefs répétées (ex: plusieurs "kafka" blocs)
            if key in result:
                if isinstance(result[key], list):
                    result[key].append(sub_result)
                else:
                    result[key] = [result[key], sub_result]
            else:
                result[key] = sub_result

        elif line == "}":
            return result, i + 1

        elif "=>" in line:
            key, value = map(str.strip, line.split("=>", 1))
            # enlever virgule finale si présente
            if value.endswith(","):
                value = value[:-1].strip()

            # inline block { ... }
            if value.startswith("{") and value.endswith("}"):
                inner = value[1:-1].strip()
                inner_lines = [l.strip() for l in inner.split(",") if l.strip()]
                inner_dict = {}
                for inner_line in inner_lines:
                    if "=>" in inner_line:
                        k, v = map(str.strip, inner_line.split("=>", 1))
                        v = v.strip('"').strip("'")
                        inner_dict[k.strip('"').strip("'")] = v
                parsed_value = inner_dict
            else:
                v = value.strip()
                # array simple ["a","b"]
                if v.startswith("[") and v.endswith("]"):
                    items = [it.strip().strip('"').strip("'") for it in v[1:-1].split(",") if it.strip()]
                    parsed_value = items
                else:
                    # quoted string
                    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                        parsed_value = v[1:-1]
                    else:
                        # entier (positif/négatif)
                        if re.fullmatch(r"-?\d+", v):
                            parsed_value = int(v)
                        else:
                            parsed_value = v

            key = key.strip('"').strip("'")
            # gérer valeurs répétées pour une même clef en les transformant en liste
            if key in result:
                if isinstance(result[key], list):
                    result[key].append(parsed_value)
                else:
                    result[key] = [result[key], parsed_value]
            else:
                result[key] = parsed_value
            i += 1
        else:
            i += 1

    return result, i

def parse_logstash_pipeline(config: str) -> dict:
    clean = remove_comments(config)
    lines = clean.splitlines()
    result, _ = parse_block(lines, 0)
    return result

config_str ="""
input {
  kafka {
    id => "tmp-copy-inte-ibm-dmzr-cloudlogs-preprd" #### c'est ça que le logstash prend en compte, le meme que le pipeline id
    bootstrap_servers => "s@1v19925078.fr.net.intra:9095, 01v19925079.fr.net.intra:9095,s01v19925080.fr.net.intra: 9895"
    auto_offset_reset => "latest"
    security_protocol => "SSL"
    ssl_keystore_password => "${keystore_pass}"
    ssl_keystore_location => "${keystore_path}"
    ssl_keystore_type => ["JKS"]
    ssl_truststore_location => "${trustore_path}"
    ssl_truststore_password => "${trustore_pass}"
    ssl_truststore_type => ["JKS"]
    consumer_threads => 1
    topics => 'prodsec-checkpoint-final'
    topics => 'prodsec-ibm-dmzr-cloudlogs-enriched' ##### TODO
    client_id => 'tmp-copy-inte-ibm-dmzr-cloudlogs' ####nom de flux
    group_id => 'tmp-copy-inte-ibm-dmzr-cloudlogs'
  }
}
filter {
  json {
    source => "message"
    remove_field => ["message"]
  }
}
output {
  kafka {
    id => "tmp-copy-inte-ibm-dmzr-cloudlogs" ##### meme nom que le pipeline
    #BEGIN PPROD
    bootstrap_servers => "s02v19941009.fr.net.intra:9092, s02v19940969.fr.net.intra:9092, s02v19941044.fr.net.intra:9092" 
    topic_id => 'prodsec-ibm-dmzr-cloudlogs-enriched' ###TOPIC de sortie
    codeo => 'json'
    client_id => 'tmp-copy-inte-ibm-dmzr-cloudlogs-preprd' ### le meme que celui d'en haut
    security_protocol => "SSL"
    ssl_keystore_password => "${keystorepass}"
    ssl_keystore_location => "${keystore_path}"
    ssl_keystore_type => ["JKS"]
    ssl_truststore_location => "${trustore_path}"
    ssl_truststore_password => "${trustore_pass}"
    ssl_truststore_type => ["JKS"]
  }
}
"""
parsed = parse_logstash_pipeline(config_str)
print(json.dumps(parsed, indent=2))
