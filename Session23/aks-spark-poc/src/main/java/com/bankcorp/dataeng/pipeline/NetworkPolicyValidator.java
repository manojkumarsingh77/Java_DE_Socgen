package com.bankcorp.dataeng.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * TOPIC: Network Policies.
 *
 * On AKS with Calico (or Azure Network Policy) enabled, k8s/01-network-policies.yaml
 * enforces a DEFAULT-DENY posture per namespace:
 *
 *   - deny-all-ingress / deny-all-egress applied to every banking-* namespace
 *   - explicit ALLOW rules punched through for:
 *       * driver <-> executor pod-to-pod traffic on port 7078 (block manager)
 *         and the ephemeral driver port range, matched via podSelector
 *         `spark-role in (driver, executor)`
 *       * egress to Azure ADLS Gen2 / Blob Storage endpoints (port 443) via
 *         an `ipBlock` matching the Azure Storage service tag CIDR ranges,
 *         OR preferably an Azure Firewall FQDN rule when using
 *         Azure CNI Overlay + Application Gateway egress
 *       * egress to the Key Vault CSI provider's private endpoint
 *       * DENY everything else, including cross-namespace traffic between
 *         banking-dev and banking-prod (the literal enforcement of
 *         Multi-Env Isolation at the network layer)
 *
 * This class cannot enforce Kubernetes NetworkPolicy locally (that's a
 * cluster-level CNI feature - Calico/Cilium - not a JVM concept), but it
 * DOES perform real TCP reachability probes against the exact endpoints the
 * policy is designed to allow/deny, and cross-references the result against
 * the declared intent - a useful pre-flight smoke test that mirrors what a
 * Kubernetes `NetworkPolicy` + `Egress` audit tool (e.g. `kubectl-trace`,
 * Cilium Hubble) would report in-cluster.
 */
public final class NetworkPolicyValidator {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkPolicyValidator.class);
    private static final int PROBE_TIMEOUT_MILLIS = 2000;

    private NetworkPolicyValidator() {
    }

    public static void validateExpectedTrafficMatrix() {
        Map<String, EndpointExpectation> matrix = new LinkedHashMap<>();
        matrix.put("adls-gen2 (dfs.core.windows.net:443)",
                new EndpointExpectation("dfs.core.windows.net", 443, true,
                        "Egress ALLOW rule: azure-storage-service-tag"));
        matrix.put("blob-storage (blob.core.windows.net:443)",
                new EndpointExpectation("blob.core.windows.net", 443, true,
                        "Egress ALLOW rule: azure-storage-service-tag"));
        matrix.put("key-vault (vault.azure.net:443)",
                new EndpointExpectation("vault.azure.net", 443, true,
                        "Egress ALLOW rule: azure-keyvault-service-tag"));
        matrix.put("public-internet-probe (example.com:443)",
                new EndpointExpectation("example.com", 443, false,
                        "Default-deny egress - no matching NetworkPolicy rule expected"));

        LOG.info("========== NETWORK POLICY EXPECTED TRAFFIC MATRIX ==========");
        matrix.forEach((label, expectation) -> {
            boolean reachable = probe(expectation.host(), expectation.port());
            String verdict = reachable == expectation.expectedAllowedOnAks()
                    ? "MATCHES POLICY INTENT"
                    : "DEVIATES FROM POLICY INTENT (expected on unrestricted local network - verify on real AKS)";
            LOG.info("{} -> reachableFromHere={} | onAksShouldBe={} | rule='{}' | {}",
                    label, reachable, expectation.expectedAllowedOnAks() ? "ALLOW" : "DENY",
                    expectation.policyRuleName(), verdict);
        });
        LOG.info("NOTE: Running locally has NO NetworkPolicy enforcement (that is a cluster CNI feature). " +
                "This matrix documents INTENT; actual enforcement is validated post-deployment via " +
                "'kubectl exec -it <spark-driver-pod> -- nc -zv <endpoint> <port>' inside each namespace.");
        LOG.info("=============================================================");
    }

    private static boolean probe(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), PROBE_TIMEOUT_MILLIS);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private record EndpointExpectation(String host, int port, boolean expectedAllowedOnAks, String policyRuleName) {
    }
}
