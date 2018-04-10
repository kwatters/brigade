package com.kmwllc.brigade.config;

public interface StageConfig extends Config<StageConfig> {

    String getStageName();
    String getStageClass();
}
