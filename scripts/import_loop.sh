#!/bin/bash
# Periodically pull the India boxes' capture logs (vpsin shard B, vpsin2 shard C) and import them into the archive index.
# Runs as sarthak (reads the pass files), stages to a tmp dir, hands to the archive user to import. The proxy route
# (shard A) writes straight to the index and needs no import. Loop until the India sweeps finish.
P=/home/sarthak/.config/woodpecker/india-vps.pass
P2=/home/sarthak/.config/woodpecker/india-vps2.pass
STAGE=/tmp/archive-india-import; mkdir -p $STAGE
while true; do
  sshpass -f $P  rsync -az -e "ssh -o ConnectTimeout=20 -o StrictHostKeyChecking=accept-new" --include='*.jsonl' --exclude='*' root@87.76.191.50:/root/archive-cap/outB/ $STAGE/ 2>/dev/null
  sshpass -f $P2 rsync -az -e "ssh -o PubkeyAuthentication=no -o ConnectTimeout=20 -o StrictHostKeyChecking=accept-new" --include='*.jsonl' --exclude='*' root@87.76.191.75:/root/archive-cap/outC/ $STAGE/ 2>/dev/null
  sudo -n cp -n $STAGE/*.jsonl /var/lib/archive/india_cap/ 2>/dev/null
  sudo -n chown archive:archive /var/lib/archive/india_cap/*.jsonl 2>/dev/null
  sudo -n -u archive bash -c 'cd /opt/archive && set -a && . /etc/archive/env && set +a && PYTHONPATH=/opt/archive .venv/bin/python -m archive.import_b2 /var/lib/archive/india_cap/' 2>&1 | tail -1
  sleep 900
done
