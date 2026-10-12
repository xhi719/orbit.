# Orbit free worker — review-only source package

This approved public repository contains only worker code and public game/source metadata. The Spark worker and free FCM dispatcher are connected separately through repository secrets. No project owner ADC, service key, user/device data, raw collection DB, images, app/UI, or native binaries are included. No reuse license is granted by this package.

Read [the concrete review plan and dated updates](docs/github-free-runner-plan.md). `MANIFEST.sha256.json` records each payload file. The workflow remains skipped for a private repository or unless `ORBIT_FIREBASE_ENABLED=true`; FCM remains disabled unless explicitly configured. No new cloud permissions or billing configuration are created by source updates.

Local checks (Python 3.11+; no external service/key needed):

```sh
python -m unittest discover -s services/firebase/tests -p test_firebase.py -v
python services/firebase/collector.py
python services/firebase/collector.py --seed --report reports/seed-dry-run.json
python services/firebase/admin.py
python services/firebase/notifications.py
```

Only after permission/Secret/Spark review, install `services/firebase/requirements.txt` and follow the plan's exact operational commands. Runtime defaults are PLAN_ONLY/DISABLED. The candidate games require official reset evidence; reviewed Steam feeds do not imply full official channel coverage. Publication timestamps never become event schedules. TASK_REMINDER is disabled. Remote launch attendance is explicit opt-in with a selected verified server, completion recheck and at most three reminders per cycle. FCM provider acceptance is not physical receipt. Native device receipt and cross-device launch detection require separate physical tests.
