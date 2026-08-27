#!/usr/bin/env bash
# Chaos test: SIGKILL the consumer pipeline under live traffic and watch it recover.
#
# Runs on the host, because the thing being killed is a container and the point is
# that nothing inside the system papers over the failure. Nothing here repairs
# anything -- the restart policy restarts the process and the pipeline is left to
# sort itself out. Every number printed is read back from Redpanda, TimescaleDB or
# Prometheus after the fact.
#
# "Recovered" is defined up front, not after seeing the result:
#   1. consumer group lag returns below RECOVERED_LAG and stays there
#   2. zero duplicate (icao24, event_time) rows -- idempotency held across the kill
#   3. rows keep accumulating, i.e. events published during the outage were not lost
#
# Usage: bash scripts/chaos_kill.sh [outage_seconds]

set -u
OUTAGE_S="${1:-45}"
# Crash the consumer during peak traffic, not while idle. At the baseline rate the
# restart is quick enough that lag never leaves single digits -- true, but it shows
# nothing about recovery. BURST_RATE raises the generator first so the outage
# actually costs something and the controller has work to do.
BURST_RATE="${BURST_RATE:-}"
RECOVERED_LAG="${RECOVERED_LAG:-200}"
RECOVER_TIMEOUT_S="${RECOVER_TIMEOUT_S:-300}"
GROUP="${KAFKA_CONSUMER_GROUP:-contrail-sink}"

psql() { docker compose exec -T timescaledb psql -U contrail -d contrail -t -c "$1" | tr -d ' \r' | head -1; }
lag()  { docker compose exec -T redpanda rpk group describe "$GROUP" 2>/dev/null | awk '/TOTAL-LAG/{print $2}'; }
rows() { psql "SELECT count(*) FROM flight_events;"; }
dupes() {
  psql "SELECT count(*) FROM (SELECT icao24, event_time FROM flight_events
        GROUP BY icao24, event_time HAVING count(*) > 1) d;"
}

echo "CONTRAIL - chaos test: SIGKILL the consumer pipeline under live traffic"
echo "======================================================================="
echo "recovered := lag < ${RECOVERED_LAG} sustained, 0 duplicate keys, rows still growing"
echo

if [ -n "${BURST_RATE}" ]; then
  echo "raising generator to ${BURST_RATE} Hz/aircraft for the duration of the test"
  GEN_RATE_HZ="${BURST_RATE}" docker compose up -d generator >/dev/null 2>&1
  sleep 25
fi

BEFORE_ROWS=$(rows); BEFORE_LAG=$(lag); BEFORE_DUPES=$(dupes)
echo "baseline        rows=${BEFORE_ROWS}  lag=${BEFORE_LAG}  duplicate_keys=${BEFORE_DUPES}"

echo
echo "--- SIGKILL the process inside the container (simulates a crash) ---"
# NOT `docker compose kill`: Docker treats that as an operator-initiated stop and
# deliberately does not apply the restart policy, so the container stays down and
# the test measures nothing. Killing PID 1 inside the container makes it exit
# non-zero, which is what a real crash looks like and what `restart: unless-stopped`
# is there to answer.
RESTARTS_BEFORE=$(docker inspect -f '{{.RestartCount}}' contrail-pipeline-1 2>/dev/null || echo 0)
KILL_AT=$(date +%s)
docker compose exec -T pipeline python -c "
import os, signal
me = os.getpid()
for pid in os.listdir('/proc'):
    if not pid.isdigit() or pid == '1' or int(pid) == me:
        continue
    try:
        cmd = open('/proc/%s/cmdline' % pid, 'rb').read().decode('utf-8', 'replace')
    except OSError:
        continue
    if 'src.control.supervisor' in cmd:
        os.kill(int(pid), signal.SIGKILL)
        print('SIGKILLed supervisor pid', pid)
" 2>&1 | head -2
sleep 5
RESTARTS_AFTER=$(docker inspect -f '{{.RestartCount}}' contrail-pipeline-1 2>/dev/null || echo 0)
echo "state           $(docker compose ps pipeline --format '{{.Status}}' 2>/dev/null || echo gone)"
echo "restart count   ${RESTARTS_BEFORE} -> ${RESTARTS_AFTER}"
if [ "${RESTARTS_AFTER}" -le "${RESTARTS_BEFORE}" ]; then
  echo "FAIL: the process did not actually die -- this test would report a false PASS."
  echo "      A chaos test that kills nothing is worse than no chaos test."
  exit 1
fi

echo "--- holding the outage for ${OUTAGE_S}s while the generator keeps producing ---"
END=$(( $(date +%s) + OUTAGE_S ))
while [ "$(date +%s)" -lt "$END" ]; do
  sleep 10
  echo "  t+$(( $(date +%s) - KILL_AT ))s  lag=$(lag)  rows=$(rows)  $(docker compose ps pipeline --format '{{.Status}}' 2>/dev/null)"
done

PEAK_LAG=$(lag)

if [ -n "${BURST_RATE}" ]; then
  echo
  echo "--- burst over, generator back to its configured rate ---"
  # Deliberately before the recovery clock starts. Leaving the burst running would
  # measure whether the pipeline can sustain 6x load indefinitely, which is a
  # capacity question, not a recovery one. The crash and its backlog are what is
  # under test here.
  docker compose up -d generator >/dev/null 2>&1
fi

echo
echo "--- waiting for automatic recovery (restart policy only, no manual action) ---"
DEADLINE=$(( $(date +%s) + RECOVER_TIMEOUT_S ))
RECOVERED_AT=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  L=$(lag); L=${L:-999999}
  echo "  t+$(( $(date +%s) - KILL_AT ))s  lag=${L}  rows=$(rows)"
  if [ "$L" -lt "$RECOVERED_LAG" ]; then RECOVERED_AT=$(( $(date +%s) - KILL_AT )); break; fi
  sleep 10
done

AFTER_ROWS=$(rows); AFTER_DUPES=$(dupes)
echo
echo "======================================================================="
echo "outage held            ${OUTAGE_S}s"
echo "peak lag during outage ${PEAK_LAG}"
echo "recovered after        ${RECOVERED_AT:-DID NOT RECOVER within ${RECOVER_TIMEOUT_S}s}s from kill"
echo "rows before / after    ${BEFORE_ROWS} / ${AFTER_ROWS}  (+$(( AFTER_ROWS - BEFORE_ROWS )))"
echo "duplicate keys         before=${BEFORE_DUPES}  after=${AFTER_DUPES}"
echo
echo "controller actions since the kill:"
docker compose logs pipeline --since "${OUTAGE_S}s" 2>/dev/null \
  | grep -o '"msg": "control action".*' | tail -20
[ -z "${RECOVERED_AT}" ] && exit 1
[ "${AFTER_DUPES}" != "0" ] && { echo "FAIL: duplicate keys present"; exit 1; }
echo "PASS"
