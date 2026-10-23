# Difficulties log

| Date | Problem | What we tried | Outcome |
| --- | --- | --- | --- |
| Oct 23 | API sample request timed out after 120 s | Tested with curl: server took ~226 s to respond to a 5-row request | Raised read timeout to 15 min |
| Oct 23 | Full download is slow (~1–3 MB/s) and restarts from 0 if stopped | Ran overnight with caffeinate; don't interrupt | In progress |