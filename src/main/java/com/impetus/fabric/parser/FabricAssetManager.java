package com.impetus.fabric.parser;

import java.util.Properties;

import com.impetus.blkch.sql.parser.AbstractAssetManager;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.fabric.model.Config;

public class FabricAssetManager extends AbstractAssetManager {
    
    private Config config;
    
    public FabricAssetManager(LogicalPlan logicalPlan, Config config) {
        this.logicalPlan = logicalPlan;
        this.config = config;
    }

    @Override
    public Properties getDBProperties() {
        return config.getDbProperties();
    }

}
