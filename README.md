# Orbit free worker — review-only source package

This is a proposed public repository containing only worker code and public game/source metadata. It is not a deployed scheduler. No project owner ADC, service key, user/device data, raw collection DB, images, app/UI, or native binaries are included. No reuse license is granted by this package.

Read [the concrete review plan](docs/github-free-runner-plan.md) before publishing or configuring credentials. `MANIFEST.sha256.json` records each payload file. The workflow remains skipped for a private repository or unless `ORBIT_FIREBASE_ENABLED=true`; FCM remains disabled unless explicitly configured. Publishing the repository is a separate user-approved action.

Local checks (Python 3.11+; no external service/key needed):

```sh
python -m unittest discover -s services/firebase/tests -p test_firebase.py -v
python services/firebase/collector.py
python services/firebase/collector.py --seed --report reports/seed-dry-run.json
python services/firebase/admin.py
python services/firebase/notifications.py
```

Only after separate permission/Secret/Spark review, install `services/firebase/requirements.txt` and follow the plan's exact operational commands. Runtime defaults are PLAN_ONLY/DISABLED. The 137 candidate games have unknown/unverified reset rules; the 49 reviewed Steam feeds do not imply full official channel coverage. Publication timestamps never become event schedules. TASK_REMINDER is disabled. FCM provider acceptance is not physical receipt.
