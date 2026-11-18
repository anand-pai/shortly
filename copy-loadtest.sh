#!/bin/sh
# Copy a locally built loadtest binary into the sleep pod and start it.
# The pod must already be running: kubectl apply -f k8s/poc/txtly-loadtest.yaml
set -eu

ns=scylla
pod=txtly-loadtest
bin=${1:-loadtest}

if [ ! -f "$bin" ]; then
  echo "missing binary: $bin" >&2
  echo "build it where Go is installed: CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o loadtest ./cmd/loadtest" >&2
  exit 1
fi

kubectl exec -n "$ns" "$pod" -- mkdir -p /code
kubectl cp "$bin" "$ns/$pod:/code/loadtest"
kubectl exec -n "$ns" "$pod" -- chmod 755 /code/loadtest
kubectl exec -n "$ns" "$pod" -- sh -c 'sms_arg=
if [ -n "${SMS_PERCENT:-}" ]; then
  sms_arg="--sms-percent $SMS_PERCENT"
fi
nohup /code/loadtest \
  --grpc-addr "$GRPC_ADDR" \
  --http-base "$HTTP_BASE" \
  --rps "$RPS" \
  --write-percent "$WRITE_PERCENT" \
  $sms_arg \
  --duration "$DURATION" \
  --company-pool "$COMPANY_POOL" \
  --workers "$WORKERS" \
  --grpc-conns "$GRPC_CONNS" \
  --http-conns "$HTTP_CONNS" \
  --warmup-slugs "$WARMUP_SLUGS" \
  --report-interval "$REPORT_INTERVAL" \
  > /tmp/loadtest.log 2>&1 &'

echo "started. follow with: kubectl exec -n $ns $pod -- tail -f /tmp/loadtest.log"
