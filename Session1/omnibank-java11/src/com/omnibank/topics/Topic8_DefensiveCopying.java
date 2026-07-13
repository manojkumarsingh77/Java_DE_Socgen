package com.omnibank.topics;

import java.util.*;

public class Topic8_DefensiveCopying {

    static final class UnsafeAccount {
        final String id;
        final Date createdOn;
        final List<String> beneficiaries;

        UnsafeAccount(String id, Date createdOn, List<String> beneficiaries) {
            this.id = id;
            this.createdOn = createdOn;
            this.beneficiaries = beneficiaries;
        }
        Date getCreatedOn() { return createdOn; }
        List<String> getBeneficiaries() { return beneficiaries; }
    }

    static final class SafeAccount {
        private final String id;
        private final Date createdOn;
        private final List<String> beneficiaries;

        SafeAccount(String id, Date createdOn, List<String> beneficiaries) {
            this.id = id;
            this.createdOn = new Date(createdOn.getTime());
            this.beneficiaries = new ArrayList<>(beneficiaries);
        }
        Date getCreatedOn() {
            return new Date(createdOn.getTime());
        }
        List<String> getBeneficiaries() {
            return Collections.unmodifiableList(beneficiaries);
        }
    }

    static final class EncryptedSecret {
        private final byte[] key;
        EncryptedSecret(byte[] key) { this.key = key.clone(); }
        byte[] getKey() { return key.clone(); }
        byte firstByte() { return key[0]; }
    }

    public static void run() {
        System.out.println("\n=========== TOPIC 8: DEFENSIVE COPYING ===========");

        Date created = new Date();
        List<String> beneficiaries = new ArrayList<>(Arrays.asList("Asha", "Ravi"));

        UnsafeAccount unsafe = new UnsafeAccount("ACC1", created, beneficiaries);

        beneficiaries.add("MALICIOUS");
        created.setTime(0);

        System.out.println("UNSAFE class state after caller mutated inputs:");
        System.out.println("  beneficiaries: " + unsafe.getBeneficiaries());
        System.out.println("  createdOn:     " + unsafe.getCreatedOn());

        unsafe.getBeneficiaries().add("HACK_VIA_GETTER");
        unsafe.getCreatedOn().setTime(1L);
        System.out.println("After mutating getter return values:");
        System.out.println("  beneficiaries: " + unsafe.getBeneficiaries());
        System.out.println("  createdOn:     " + unsafe.getCreatedOn());

        Date safeDate = new Date();
        List<String> safeBens = new ArrayList<>(Arrays.asList("Asha", "Ravi"));
        SafeAccount safe = new SafeAccount("ACC2", safeDate, safeBens);

        safeBens.add("MALICIOUS");
        safeDate.setTime(0);
        try { safe.getBeneficiaries().add("HACK_VIA_GETTER"); }
        catch (UnsupportedOperationException e) {
            System.out.println("\nSAFE class blocked add via getter (unmodifiable list).");
        }
        safe.getCreatedOn().setTime(1L);

        System.out.println("SAFE class state after all attacks:");
        System.out.println("  beneficiaries: " + safe.getBeneficiaries() + "  (unchanged)");
        System.out.println("  createdOn:     " + safe.getCreatedOn()     + "  (unchanged)");

        byte[] key = {1,2,3,4,5};
        EncryptedSecret s = new EncryptedSecret(key);
        key[0] = 99;
        System.out.println("\nKey first byte after tamper attempt: " + s.firstByte()
                + " (should be 1, defensive copy preserved it)");
    }
}
