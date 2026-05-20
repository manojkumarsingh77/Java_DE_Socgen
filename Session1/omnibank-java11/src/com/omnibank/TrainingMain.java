package com.omnibank;

import com.omnibank.topics.*;

/**
 * OmniBank Training Driver - Java 11 compatible.
 */
public class TrainingMain {
    public static void main(String[] args) throws Exception {
        System.out.println("================================================");
        System.out.println("   OMNIBANK ADVANCED JAVA TRAINING - ALL TOPICS  ");
        System.out.println("================================================");

        Topic1_FunctionalInterfaces.run();
        Topic2_CustomCollectors.run();
        Topic3_SpliteratorMechanics.run();
        Topic4_StreamFusion.run();
        Topic5_ImmutableDTODesign.run();
        Topic6_MemoryFootprint.run();
        Topic7_ObjectPooling.run();
        Topic8_DefensiveCopying.run();

        System.out.println("\n================================================");
        System.out.println("            ALL TOPICS COMPLETED                 ");
        System.out.println("================================================");
    }
}
