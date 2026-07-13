package com.retail;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;

public class DeleteParquetOutput {

    public static void main(String[] args) {

        String outputPath = "data/parquet/orders";

        Path directory = Paths.get(outputPath);

        if (!Files.exists(directory)) {
            System.out.println("No Parquet output found.");
            return;
        }

        try {
            Files.walk(directory)
                    .sorted(Comparator.reverseOrder()) // Delete files before directories
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                            System.out.println("Deleted: " + path);
                        } catch (IOException e) {
                            System.err.println("Failed to delete: " + path);
                            e.printStackTrace();
                        }
                    });

            System.out.println("\n========================================");
            System.out.println("Parquet output deleted successfully.");
            System.out.println("========================================");

        } catch (IOException e) {
            System.err.println("Error while deleting Parquet output.");
            e.printStackTrace();
        }
    }
}