path = '/home/phil/data_server/source/parquet_writer/parquet_writer.cpp'
with open(path, encoding='utf-8') as f:
    src = f.read()

if 'g_seen_topics_mutex' in src:
    print('already patched')
    exit(0)

# 1. Add seen-topics globals after g_sync_session_id
OLD1 = 'static uint64_t g_sync_drops_detected {0};\nstatic std::string g_sync_session_id;'
NEW1 = (
    'static uint64_t g_sync_drops_detected {0};\n'
    'static std::string g_sync_session_id;\n'
    '\n'
    '// Seen topics (up to point_name, dtype_hint stripped) — for health endpoint\n'
    'static std::mutex                       g_seen_topics_mutex;\n'
    'static std::vector<std::string>         g_seen_topics_list;   // insertion order\n'
    'static std::unordered_set<std::string>  g_seen_topics_set;    // dedup'
)
assert OLD1 in src, 'patch 1 anchor not found'
src = src.replace(OLD1, NEW1, 1)
print('patch 1 ok (globals)')

# 2. Register topic in process_message after `if (!info_opt) return;`
OLD2 = '    if (!info_opt) return;\n\n    Row row = parse_payload(payload_str, *info_opt, *g_cfg);'
NEW2 = (
    '    if (!info_opt) return;\n'
    '\n'
    '    // Record seen topic (up to point_name — drop dtype_hint segment if present)\n'
    '    {\n'
    "        std::string seen = topic_str;\n"
    "        if (!info_opt->dtype_hint.empty()) {\n"
    "            auto pos = seen.rfind('/');\n"
    "            if (pos != std::string::npos) seen = seen.substr(0, pos);\n"
    '        }\n'
    '        std::lock_guard<std::mutex> lk(g_seen_topics_mutex);\n'
    '        if (g_seen_topics_set.find(seen) == g_seen_topics_set.end() &&\n'
    '            g_seen_topics_list.size() < 200) {\n'
    '            g_seen_topics_set.insert(seen);\n'
    '            g_seen_topics_list.push_back(seen);\n'
    '        }\n'
    '    }\n'
    '\n'
    '    Row row = parse_payload(payload_str, *info_opt, *g_cfg);'
)
assert OLD2 in src, 'patch 2 anchor not found'
src = src.replace(OLD2, NEW2, 1)
print('patch 2 ok (process_message registration)')

# 3. Add seen_topics array to health JSON
# Use the exact substring we know from the debug output:
#   '    << ","\n             << "\\"disk_free_gb\\":"        << disk_gb\n             << "}";\n'
# We want to match from the leading spaces of the disk_free_gb line through the closing "}";
import re
# Match: leading spaces + << "\"disk_free_gb\":" ... disk_gb NL spaces << "}";
m = re.search(
    r'( +<< "\\"disk_free_gb\\":[^\n]+\n +<< "\}";\n)',
    src
)
if m:
    OLD3 = m.group(0)
    print('OLD3:', repr(OLD3))
    NEW3 = (
        '             << "\\\"disk_free_gb\\\":" << disk_gb\n'
        '             << ",";\n'
        '\n'
        '        // Append seen_topics array\n'
        '        std::vector<std::string> topics_snap;\n'
        '        {\n'
        '            std::lock_guard<std::mutex> lk(g_seen_topics_mutex);\n'
        '            topics_snap = g_seen_topics_list;\n'
        '        }\n'
        '        body << "\\\"seen_topics\\\":[";\n'
        '        for (size_t i = 0; i < topics_snap.size(); ++i) {\n'
        '            if (i) body << ",";\n'
        '            body << "\\\"" << topics_snap[i] << "\\\"";\n'
        '        }\n'
        '        body << "]}";\n'
    )
    src = src.replace(OLD3, NEW3, 1)
    print('patch 3 ok (health JSON)')
else:
    print('patch 3 regex failed')
    import sys; sys.exit(1)

with open(path, 'w', encoding='utf-8') as f:
    f.write(src)
print(f'done — {len(src)} bytes')
