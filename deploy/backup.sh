#!/bin/sh
# Backup diário do banco (cópia consistente com o SQLite online backup), guarda 7 dias.
# crontab: 15 4 * * * /root/radar/deploy/backup.sh
set -eu
cd "$(dirname "$0")"
mkdir -p backups
docker exec radar-app python -c "import sqlite3;s=sqlite3.connect('/app/dados/radar.db');d=sqlite3.connect('/app/dados/backup.db');s.backup(d);d.close()"
mv dados/backup.db "backups/radar-$(date +%F).db"
find backups -name 'radar-*.db' -mtime +7 -delete
