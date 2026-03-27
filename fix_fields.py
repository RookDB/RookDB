import os
content = open("src/bin/workload_benchmark.rs").read()
content = content.replace("page_id:", "page_no:")
content = content.replace("slot_id:", "item_id:")
content = content.replace("as u16", "")
with open("src/bin/workload_benchmark.rs", "w") as f:
    f.write(content)
