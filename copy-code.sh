for i in $(seq 0 14); do
  kubectl exec -n scylla txtly-app-$i -- mkdir -p /code
  kubectl cp mainfile scylla/txtly-app-$i:/code/mainfile
  kubectl exec -n scylla txtly-app-$i -- chmod 755 /code/mainfile
  kubectl exec -n scylla txtly-app-$i -- sh -c \
    '/code/mainfile > /tmp/mainfile.log 2>&1 &'
done