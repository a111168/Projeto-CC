#!/usr/bin/env bash

set -u

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
work_dir="${TMPDIR:-/tmp}/tp2-load-test"
broker_pid=""
sub1_pid=""
sub2_pid=""
pub_pid=""
tcpdump_pid=""

count="${COUNT:-1000}"
rate="${RATE:-100}"

cleanup() {
    for pid in "$pub_pid" "$sub1_pid" "$sub2_pid" "$broker_pid" "$tcpdump_pid"; do
        if [[ "$pid" =~ ^[0-9]+$ ]]; then
            kill "$pid" 2>/dev/null || true
        fi
    done
}

trap cleanup EXIT INT TERM

rm -rf "$work_dir"
mkdir -p "$work_dir"

cat > "$work_dir/broker.conf" <<EOF
bind = 127.0.0.1
control_port = 7310
data_port = 7311
duration = 30
EOF

cat > "$work_dir/pub.conf" <<EOF
broker_control = 127.0.0.1:7310
log = $work_dir/pub.log
start_after = 2
drain = 5
publish = north/station7/temperature qos=1 count=$count rate=$rate
EOF

cat > "$work_dir/sub1.conf" <<EOF
broker_control = 127.0.0.1:7310
log = $work_dir/sub1.log
duration = 20
subscribe = north/station7/temperature qos=1
EOF

cat > "$work_dir/sub2.conf" <<EOF
broker_control = 127.0.0.1:7310
log = $work_dir/sub2.log
duration = 20
subscribe = north/* qos=1
EOF

"$repo_root/run.sh" broker \
    --id broker1 \
    --config "$work_dir/broker.conf" \
    > "$work_dir/broker.out" 2>&1 &
broker_pid=$!

sleep 1

"$repo_root/run.sh" subscriber \
    --id sub1 \
    --config "$work_dir/sub1.conf" \
    > "$work_dir/sub1.out" 2>&1 &
sub1_pid=$!

"$repo_root/run.sh" subscriber \
    --id sub2 \
    --config "$work_dir/sub2.conf" \
    > "$work_dir/sub2.out" 2>&1 &
sub2_pid=$!

sleep 1

if command -v tcpdump >/dev/null 2>&1; then
    tcpdump -i lo -nn 'udp port 7311' -w "$work_dir/eventcast.pcap" \
        > "$work_dir/tcpdump.out" 2>&1 &
    tcpdump_pid=$!
fi

"$repo_root/run.sh" publisher \
    --id pub1 \
    --config "$work_dir/pub.conf" \
    > "$work_dir/pub.out" 2>&1 &
pub_pid=$!

wait "$pub_pid"
pub_status=$?
wait "$sub1_pid"
sub1_status=$?
wait "$sub2_pid"
sub2_status=$?

printf 'Load test results\n'
printf '%s\n' '================='
printf 'Configuration: %s messages at %s messages/second\n' "$count" "$rate"
printf 'Process status: publisher=%s subscriber1=%s subscriber2=%s\n' \
    "$pub_status" "$sub1_status" "$sub2_status"
printf 'Published:      %s\n' "$(wc -l < "$work_dir/pub.log")"
printf 'Received sub1:  %s\n' "$(wc -l < "$work_dir/sub1.log")"
printf 'Received sub2:  %s\n' "$(wc -l < "$work_dir/sub2.log")"

if [[ -f "$work_dir/eventcast.pcap" ]] && command -v tcpdump >/dev/null 2>&1; then
    packet_count="$(tcpdump -nn -r "$work_dir/eventcast.pcap" 2>/dev/null | wc -l)"
    printf 'UDP packets:    %s\n' "$packet_count"
else
    printf 'UDP packets:    capture unavailable (tcpdump not installed or not permitted)\n'
fi

printf 'Artifacts:       %s\n' "$work_dir"
