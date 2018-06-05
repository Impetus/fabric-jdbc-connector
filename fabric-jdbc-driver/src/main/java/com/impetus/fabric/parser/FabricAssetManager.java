/*******************************************************************************
 * * Copyright 2018 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/

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
