package com.impetus.fabric.parser;

import java.util.Properties;

import com.impetus.blkch.sql.parser.AbstractAssetCreator;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.fabric.model.Config;

public class FabricAssetCreator extends AbstractAssetCreator {
    
    private Config config;
    
    public FabricAssetCreator(LogicalPlan logicalPlan, Config config) {
        this.logicalPlan = logicalPlan;
        this.config = config;
    }

    @Override
    public Properties getDBProperties() {
        return config.getDbProperties();
    }

}
