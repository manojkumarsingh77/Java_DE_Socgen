# Dev environment: no Kubernetes manifests

Dev is deliberately **IntelliJ-local-only** — it runs with `master = "local[*]"`
against the local filesystem (`/tmp/retail-platform/dev/...`) using
`local-env` secrets. This keeps the inner developer loop (edit → run → see
result) under a few seconds, with zero Azure cost and zero cluster
dependency.

There is nothing wrong with also running the packaged jar in a personal AKS
dev namespace if you want to test container behavior early — to do that,
copy `k8s/test/*.yaml`, replace `test`/`retail-platform-test` with
`dev`/`retail-platform-dev`, and point `secrets.provider` back to
`azure-key-vault` with a dev Key Vault. This project does not ship that
variant to keep the promotion path unambiguous: **local → test → prod**.
