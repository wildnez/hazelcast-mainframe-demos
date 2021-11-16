package com.hazelcast.certification.domain;

import java.io.Serializable;
import java.util.Random;

public class Merchant implements Serializable {

    private String merchantID;
    private String merchantName;
    private int reputation;

    private double avgTxnAmount;

    private String location;
    private Random random = new Random();

    public Merchant(String merchantID) {
        this.merchantID = merchantID;
        this.merchantName = "MerchantDB" + merchantID;
        avgTxnAmount = pricePoints[random.nextInt(4)];
        reputation = random.nextInt(10);
    }

    public Merchant() {}

    public Random getRandom() {
        return random;
    }

    public void setRandom(Random random) {
        this.random = random;
    }

    public String getMerchantID() { return merchantID; }

    public void setMerchantID(String id) { merchantID = id; }
    public Merchant getObject() { return this; }

    public String getMerchantName() { return merchantName; }

    public void setMerchantName(String name) { merchantName = name; }
    public int getReputation() { return reputation; }

    public void setReputation(int reputation) { this.reputation = reputation; }
    public String getLocation() { return location; }

    public void setLocation(String geohash) { location = geohash; }

    public double getAvgTxnAmount() {
        return avgTxnAmount;
    }

    public void setAvgTxnAmount(double avgTxnAmount) {
        this.avgTxnAmount = avgTxnAmount;
    }


    static final int[] pricePoints = new int[] { 10, 25, 50, 100, 500, 1000 };

    // Not truly random .. will be normally distributed around average
    public double getRandomTransactionAmount() {
        int stddev = (int) avgTxnAmount / 5;
        double amount = random.nextGaussian() * stddev + avgTxnAmount;
        return amount;
    }

    public enum RISK { LOW, MEDIUM, HIGH }
}
